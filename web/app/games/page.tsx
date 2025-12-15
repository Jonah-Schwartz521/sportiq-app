"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, getTeamLabel, NHL_TEAM_NAMES } from "@/lib/teams";

function getLocalISODate(date: Date = new Date()): string {
  // Build YYYY-MM-DD using the local calendar date (no UTC conversion)
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}


type SportFilterId = "all" | 1 | 2 | 3 | 4 | 5;

// Format American odds for display (e.g. 182 -> "+182", -140 -> "-140")
function formatAmericanOdds(value: number | null | undefined): string | null {
  if (value === null || value === undefined || Number.isNaN(value)) return null;
  return value > 0 ? `+${value}` : `${value}`;
}

type OddsRecord = {
  sport: string;
  commence_time_utc: string;
  home_team: string;
  away_team: string;
  bookmaker: string;
  market: string;
  outcome_name: string;
  outcome_price: number;
  point?: number | null;
  last_update_utc: string;
  source: string;
};

type ApiGameOdds = {
  home_team: string;
  away_team: string;
  commence_time_utc: string;
  moneyline: OddsRecord[];
  spreads: OddsRecord[];
};

type AggregatedOdds = {
  moneyline: {
    home: number | null;
    away: number | null;
    bookmakers: string[];
  };
  spreads: {
    home: { point: number | null; price: number | null };
    away: { point: number | null; price: number | null };
    bookmakers: string[];
  };
};

const ODDS_FETCH_HOURS = 48; // default window: next 48 hours

function normalizeTeamForMatch(name: string | null | undefined): string {
  if (!name) return "";
  const cleaned = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  // Handle common NBA aliases
  const replacements: Record<string, string> = {
    "la clippers": "los angeles clippers",
    "la lakers": "los angeles lakers",
    "ny knicks": "new york knicks",
    "ny nets": "brooklyn nets",
    "phoenix suns": "phoenix suns", // keep explicit entry to avoid fallthrough
  };

  return replacements[cleaned] ?? cleaned;
}

function buildGameKey(home: string, away: string): string {
  return `${normalizeTeamForMatch(home)}|${normalizeTeamForMatch(away)}`;
}

function parseTimeTo24h(timeStr: string | null | undefined): string | null {
  if (!timeStr) return null;
  const match = timeStr
    .trim()
    .match(/(\d{1,2}):(\d{2})(?:\s*(AM|PM))?/i);

  if (!match) return null;

  let hour = parseInt(match[1], 10);
  const minute = match[2];
  const ampm = match[3]?.toUpperCase() ?? null;

  if (ampm === "PM" && hour < 12) hour += 12;
  if (ampm === "AM" && hour === 12) hour = 0;

  const hh = String(hour).padStart(2, "0");
  return `${hh}:${minute}`;
}

function getEventStartDateTime(
  dateStr: string | null | undefined,
  timeStr: string | null | undefined,
): Date | null {
  if (!dateStr) return null;
  const hhmm = parseTimeTo24h(timeStr) ?? "00:00";
  const iso = `${dateStr}T${hhmm}:00Z`;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function withinTwoHours(a: Date | null, b: Date | null): boolean {
  if (!a || !b) return true; // if uncertain, allow match
  const diff = Math.abs(a.getTime() - b.getTime());
  return diff <= 2 * 60 * 60 * 1000;
}

function median(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((x, y) => x - y);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function aggregateGameOdds(
  game: ApiGameOdds,
  homeTeam: string,
  awayTeam: string,
): AggregatedOdds {
  const moneylineBooks = new Set<string>();
  let bestHome: number | null = null;
  let bestAway: number | null = null;

  for (const row of game.moneyline) {
    const outcome = normalizeTeamForMatch(row.outcome_name);
    const isHome =
      outcome === normalizeTeamForMatch(homeTeam) || outcome === "home";
    const isAway =
      outcome === normalizeTeamForMatch(awayTeam) || outcome === "away";
    if (!isHome && !isAway) continue;

    const price = Number(row.outcome_price);
    if (!Number.isFinite(price)) continue;
    moneylineBooks.add(row.bookmaker);

    if (isHome) {
      if (bestHome === null || price > bestHome) {
        bestHome = price;
      }
    } else if (isAway) {
      if (bestAway === null || price > bestAway) {
        bestAway = price;
      }
    }
  }

  const spreadBooks = new Set<string>();
  const homeSpreads: { point: number; price: number; book: string }[] = [];
  const awaySpreads: { point: number; price: number; book: string }[] = [];

  for (const row of game.spreads) {
    const outcome = normalizeTeamForMatch(row.outcome_name);
    const isHome =
      outcome === normalizeTeamForMatch(homeTeam) || outcome === "home";
    const isAway =
      outcome === normalizeTeamForMatch(awayTeam) || outcome === "away";
    if (!isHome && !isAway) continue;

    const point =
      row.point === null || row.point === undefined
        ? null
        : Number(row.point);
    const price = Number(row.outcome_price);
    if (!Number.isFinite(price)) continue;
    if (point === null || Number.isNaN(point)) continue;

    const target = { point, price, book: row.bookmaker };
    spreadBooks.add(row.bookmaker);
    if (isHome) {
      homeSpreads.push(target);
    } else if (isAway) {
      awaySpreads.push(target);
    }
  }

  const homePoint = median(homeSpreads.map((s) => s.point));
  const awayPoint = median(awaySpreads.map((s) => s.point));

  const pickPrice = (
    spreads: { point: number; price: number; book: string }[],
    targetPoint: number | null,
  ): number | null => {
    if (spreads.length === 0 || targetPoint === null) return null;
    let closest = spreads[0];
    let bestDiff = Math.abs(spreads[0].point - targetPoint);
    for (const s of spreads) {
      const diff = Math.abs(s.point - targetPoint);
      if (diff < bestDiff) {
        closest = s;
        bestDiff = diff;
      }
    }
    return closest.price;
  };

  return {
    moneyline: {
      home: bestHome,
      away: bestAway,
      bookmakers: Array.from(moneylineBooks).slice(0, 2),
    },
    spreads: {
      home: {
        point: homePoint,
        price: pickPrice(homeSpreads, homePoint),
      },
      away: {
        point: awayPoint,
        price: pickPrice(awaySpreads, awayPoint),
      },
      bookmakers: Array.from(spreadBooks).slice(0, 2),
    },
  };
}

function findMatchingOddsGame(
  index: Map<string, ApiGameOdds[]>,
  homeTeam: string,
  awayTeam: string,
  eventDate: string | null | undefined,
  eventTime: string | null | undefined,
): ApiGameOdds | null {
  const key = buildGameKey(homeTeam, awayTeam);
  const candidates = index.get(key);
  if (!candidates || candidates.length === 0) return null;

  const eventStart = getEventStartDateTime(eventDate, eventTime);

  for (const game of candidates) {
    const commence = new Date(game.commence_time_utc);
    if (Number.isNaN(commence.getTime())) continue;
    if (withinTwoHours(eventStart, commence)) return game;
  }

  // If none fall within the tolerance, fall back to first candidate
  return candidates[0];
}

// --- Date / year helpers ---
function getYearFromDate(dateStr: string | null | undefined): string | null {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) {
    return dateStr.slice(0, 4);
  }
  return String(d.getUTCFullYear());
}

function formatReadableDate(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const parts = dateStr.split("-").map((p) => parseInt(p, 10));
  if (parts.length !== 3 || parts.some((n) => Number.isNaN(n))) {
    return dateStr;
  }
  const [year, month, day] = parts;
  const d = new Date(year, month - 1, day); // local date
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

// Format time string from 24-hour format (e.g., "19:00") to 12-hour (e.g., "7:00 PM")
function formatTime(timeStr: string | null | undefined): string | null {
  if (!timeStr) return null;

  try {
    // Handle "HH:MM" format
    const parts = timeStr.split(":");
    if (parts.length < 2) return timeStr;

    let hour = parseInt(parts[0], 10);
    const minute = parts[1].split(" ")[0]; // handle "19:00" or "7:00 PM" formats

    if (Number.isNaN(hour)) return timeStr;

    const ampm = hour >= 12 ? "PM" : "AM";
    hour = hour % 12 || 12; // Convert to 12-hour format (0 -> 12)

    return `${hour}:${minute} ${ampm}`;
  } catch {
    return timeStr;
  }
}

// Format date with time for display. If it's today, show "Today · TIME", else show full date.
function formatDateWithTime(
  dateStr: string | null,
  timeStr: string | null | undefined
): string | null {
  if (!dateStr) return null;

  const today = getLocalISODate();
  const isToday = dateStr === today;

  if (isToday && timeStr) {
    const formattedTime = formatTime(timeStr);
    return formattedTime ? `Today · ${formattedTime}` : "Today";
  }

  if (isToday) {
    return "Today";
  }

  // Not today - show full date
  const readableDate = formatReadableDate(dateStr);
  if (timeStr) {
    const formattedTime = formatTime(timeStr);
    return formattedTime ? `${readableDate} · ${formattedTime}` : readableDate;
  }

  return readableDate;
}

// UI-only status helper, so we can style FINAL / LIVE / UPCOMING
function getEventStatus(
  e: Event,
  isFinal: boolean,
): "final" | "upcoming" | "live" {
  const status = (e.status ?? "").toString().toLowerCase();
  // Trust the backend's status first
  if (status === "in_progress" || status === "live") return "live";
  if (status === "final") return "final";
  if (status === "upcoming") return "upcoming";

  // Legacy fallback for old API responses
  if (isFinal) return "final";
  return "upcoming";
}

// Sport filter config shared by main page + summary
const SPORT_FILTERS: { id: SportFilterId; label: string }[] = [
  { id: "all", label: "All sports" },
  { id: 1, label: "NBA" },
  { id: 2, label: "MLB" },
  { id: 3, label: "NFL" },
  { id: 4, label: "NHL" },
  { id: 5, label: "UFC" },
];

function getSportLabelFromFilterId(id: SportFilterId): string {
  if (id === "all") return "All Sports";
  const found = SPORT_FILTERS.find((f) => f.id === id);
  return found?.label ?? "Sport";
}

// NFL abbreviations → full team names (includes legacy codes and common aliases)
const NFL_TEAM_FULL_NAME_BY_ABBR: Record<string, string> = {
  // Core 32 franchises (current abbreviations)
  ARI: "Arizona Cardinals",
  ATL: "Atlanta Falcons",
  BAL: "Baltimore Ravens",
  BUF: "Buffalo Bills",
  CAR: "Carolina Panthers",
  CHI: "Chicago Bears",
  CIN: "Cincinnati Bengals",
  CLE: "Cleveland Browns",
  DAL: "Dallas Cowboys",
  DEN: "Denver Broncos",
  DET: "Detroit Lions",
  GB: "Green Bay Packers",
  HOU: "Houston Texans",
  IND: "Indianapolis Colts",
  JAX: "Jacksonville Jaguars",
  KC: "Kansas City Chiefs",
  LA: "Los Angeles Rams", // older Rams code in data
  LAC: "Los Angeles Chargers",
  LV: "Las Vegas Raiders",
  MIA: "Miami Dolphins",
  MIN: "Minnesota Vikings",
  NE: "New England Patriots",
  NO: "New Orleans Saints",
  NYG: "New York Giants",
  NYJ: "New York Jets",
  PHI: "Philadelphia Eagles",
  PIT: "Pittsburgh Steelers",
  SEA: "Seattle Seahawks",
  SF: "San Francisco 49ers",
  TB: "Tampa Bay Buccaneers",
  TEN: "Tennessee Titans",
  WAS: "Washington Commanders",
  WSH: "Washington Commanders",

  // Explicit legacy city/team codes present in the historical data
  OAK: "Oakland Raiders",
  SD: "San Diego Chargers",
  STL: "St. Louis Rams",

  // Extra aliases / variations that might appear in either the games
  // table or the teams table. These are defensive mappings so that if
  // the event data uses these instead of the standard 2–3 letter code,
  // the UI still shows a full franchise name.

  // Carolina
  CA: "Carolina Panthers",
  CAROLINA: "Carolina Panthers",

  // Dallas
  COWBOYS: "Dallas Cowboys",

  // Detroit
  LIONS: "Detroit Lions",

  // Patriots
  NWE: "New England Patriots",

  // Giants
  GIANTS: "New York Giants",
  NY: "New York Giants",

  // Raiders
  RAIDERS: "Las Vegas Raiders",
  OAKLAND: "Oakland Raiders",

  // Eagles
  EAGLES: "Philadelphia Eagles",

  // 49ers
  NINERS: "San Francisco 49ers",
  SFO: "San Francisco 49ers",
};

// Parse an NFL event_id like "2019_01_ARI_PHI" and return full team names.
// Some rows may have numeric or unexpected ids, so we guard on type first.
function deriveNflNamesFromEventId(eventId: unknown) {
  if (typeof eventId !== "string" || !eventId) {
    return { home: null as string | null, away: null as string | null };
  }

  const parts = eventId.split("_");
  // Expected shape like "2019_01_ARI_PHI"
  if (parts.length < 4) {
    return { home: null as string | null, away: null as string | null };
  }

  const awayAbbr = parts[2];
  const homeAbbr = parts[3];

  const mapAbbrToFull = (abbr: string | undefined): string | null => {
    if (!abbr) return null;
    const key = abbr.toUpperCase();
    return NFL_TEAM_FULL_NAME_BY_ABBR[key] ?? abbr;
  };

  return {
    home: mapAbbrToFull(homeAbbr),
    away: mapAbbrToFull(awayAbbr),
  };
}

// =========================
// Subcomponent: SummaryBar
// =========================
interface SummaryBarProps {
  selectedSport: SportFilterId;
  visibleCount: number;
  dateFilter: string | null;
  yearFilter: string;
}

function SummaryBar({
  selectedSport,
  visibleCount,
  dateFilter,
  yearFilter,
}: SummaryBarProps) {
  const sportLabel = getSportLabelFromFilterId(selectedSport);

  const dateLabel =
    dateFilter != null
      ? formatReadableDate(dateFilter)
      : yearFilter !== "all"
      ? `${yearFilter} season`
      : "All dates";

  return (
    <section className="px-1">
      <div className="flex flex-wrap items-center justify-between gap-3 text-sm">
        <div className="flex items-center gap-3">
          <span className="text-2xl font-bold text-white">
            {visibleCount.toLocaleString()}
          </span>
          <div className="flex flex-col">
            <span className="text-sm font-medium text-zinc-300">
              {visibleCount === 1 ? "Game" : "Games"}
            </span>
            <span className="text-xs text-zinc-500">
              {sportLabel} · {dateLabel ?? "All time"}
            </span>
          </div>
        </div>
      </div>
    </section>
  );
}

// =========================
// Subcomponent: Final score
// =========================
interface FinalScoreBlockProps {
  homeTeam: string;
  awayTeam: string;
  homeScore: number;
  awayScore: number;
  homeIsWinner: boolean;
  awayIsWinner: boolean;
  homeWin: boolean | null;
}

function FinalScoreBlock({
  homeTeam,
  awayTeam,
  homeScore,
  awayScore,
  homeIsWinner,
  awayIsWinner,
  homeWin,
}: FinalScoreBlockProps) {
  return (
    <div className="space-y-3">
      {/* Score display */}
      <div className="flex items-center justify-center gap-4 rounded-lg bg-zinc-900/30 py-6">
        <div className="flex flex-col items-center gap-1">
          <span className={
            "text-4xl font-bold tabular-nums " +
            (awayIsWinner ? "text-emerald-400" : "text-zinc-400")
          }>
            {awayScore}
          </span>
          <span className="text-[10px] uppercase tracking-wider text-zinc-600">
            Away
          </span>
        </div>
        <span className="text-xl font-bold text-zinc-600">—</span>
        <div className="flex flex-col items-center gap-1">
          <span className={
            "text-4xl font-bold tabular-nums " +
            (homeIsWinner ? "text-emerald-400" : "text-zinc-400")
          }>
            {homeScore}
          </span>
          <span className="text-[10px] uppercase tracking-wider text-zinc-600">
            Home
          </span>
        </div>
      </div>

      {/* Winner indicator */}
      {homeWin !== null && (
        <div className="flex justify-center">
          <div className="flex items-center gap-1.5 rounded-md bg-emerald-500/10 px-3 py-1.5 text-xs font-medium text-emerald-400">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
            <span>{homeWin ? homeTeam : awayTeam} wins</span>
          </div>
        </div>
      )}
    </div>
  );
}

// =========================
// Subcomponent: UFC Result
// =========================
interface UfcResultBlockProps {
  homeTeam: string;
  awayTeam: string;
  homeWin: boolean | null;
  event: Event;
}

function UfcResultBlock({
  homeTeam,
  awayTeam,
  homeWin,
  event,
}: UfcResultBlockProps) {
  // Determine winner name
  const winnerName = homeWin === true ? homeTeam : homeWin === false ? awayTeam : null;

  // Try to get method and round from event metadata (if available from parquet)
  const rawMethod = (event as any).method ?? (event as any).win_method ?? (event as any).result ?? (event as any).finish ?? null;
  const finishRound = (event as any).finish_round ?? (event as any).win_round ?? (event as any).round ?? null;

  // Helper: Normalize and clean UFC method strings
  const normalizeMethod = (raw: any): string | null => {
    if (!raw || raw === "null" || raw === "undefined") return null;

    const str = String(raw).trim();
    if (!str) return null;

    // Extract method from strings like "TKO (Punches)" -> "TKO"
    const mainMethod = str.split("(")[0].trim();

    // Normalize common variations
    const upper = mainMethod.toUpperCase();
    if (upper.includes("KO") || upper.includes("T.K.O")) return "KO/TKO";
    if (upper.includes("SUB")) return "Submission";
    if (upper.includes("DEC")) return "Decision";
    if (upper === "U-DEC") return "Decision";
    if (upper === "S-DEC") return "Decision";
    if (upper === "M-DEC") return "Decision";

    // Return cleaned string (capitalized first letter)
    return mainMethod.charAt(0).toUpperCase() + mainMethod.slice(1).toLowerCase();
  };

  // Helper: Format round as "(R1)", "(R2)", etc.
  const formatRound = (raw: any): string | null => {
    if (!raw || raw === "null" || raw === "undefined") return null;

    const roundNum = typeof raw === 'number' ? raw : parseFloat(String(raw));
    if (isNaN(roundNum)) return null;

    return `R${Math.floor(roundNum)}`;
  };

  const method = normalizeMethod(rawMethod);
  const round = formatRound(finishRound);

  const isHomeWinner = homeWin === true;
  const isAwayWinner = homeWin === false;

  return (
    <div className="space-y-3">
      {/* Result display */}
      <div className="rounded-lg bg-zinc-900/30 p-4">
        <div className="space-y-3">
          {/* Away fighter */}
          <div className={
            "flex items-center justify-between rounded-md p-2 transition-all " +
            (isAwayWinner ? "bg-emerald-500/10" : "")
          }>
            <span className={
              "text-sm font-medium " +
              (isAwayWinner ? "text-emerald-400" : "text-zinc-400")
            }>
              {awayTeam}
            </span>
            {isAwayWinner && (
              <span className="text-lg text-emerald-400">✓</span>
            )}
          </div>

          {/* Home fighter */}
          <div className={
            "flex items-center justify-between rounded-md p-2 transition-all " +
            (isHomeWinner ? "bg-emerald-500/10" : "")
          }>
            <span className={
              "text-sm font-medium " +
              (isHomeWinner ? "text-emerald-400" : "text-zinc-400")
            }>
              {homeTeam}
            </span>
            {isHomeWinner && (
              <span className="text-lg text-emerald-400">✓</span>
            )}
          </div>
        </div>
      </div>

      {/* Result metadata */}
      {winnerName && (
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center gap-1.5 text-xs text-emerald-400">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
            <span className="font-medium">{winnerName} wins</span>
          </div>
          {(method || round) && (
            <div className="text-xs text-zinc-500">
              {method && <span>{method}</span>}
              {method && round && <span> · </span>}
              {round && <span>{round}</span>}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// =========================
// Subcomponent: Odds block
// =========================
interface OddsBlockProps {
  homeValue: string | null;
  awayValue: string | null;
  oddsLabel: string | null;
  modelHomeFormatted: string | null;
  modelAwayFormatted: string | null;
}

function OddsBlock({
  homeValue,
  awayValue,
  oddsLabel,
  modelHomeFormatted,
  modelAwayFormatted,
}: OddsBlockProps) {
  const showValueRow = homeValue !== null || awayValue !== null;
  const sourceLabel =
    oddsLabel === "Sportsbook odds"
      ? "Market snapshot"
      : oddsLabel === "Model odds"
      ? "Model view"
      : oddsLabel || null;
  const showModelCompare =
    oddsLabel === "Sportsbook odds" &&
    (modelHomeFormatted || modelAwayFormatted);

  if (!showValueRow) return null;

  return (
    <div className="space-y-2">
      <div className="rounded-xl border border-zinc-800/70 bg-zinc-900/50 p-4">
        <div className="flex items-center justify-between text-[11px] uppercase tracking-wide text-zinc-500">
          <span title="Win-outcome odds for each team">Moneyline</span>
          {sourceLabel && (
            <span className="text-xs font-medium normal-case tracking-normal text-zinc-400">
              {sourceLabel}
            </span>
          )}
        </div>

        <div className="grid grid-cols-2 gap-3 pt-3">
          <div className="rounded-lg p-2 transition-colors hover:bg-zinc-900/70">
            <div className="flex items-baseline justify-between">
              <span className="text-xs font-medium text-zinc-400">Away</span>
              <span
                className={
                  "text-lg font-semibold tabular-nums " +
                  (awayValue ? "text-zinc-100" : "text-zinc-600")
                }
              >
                {awayValue ?? "—"}
              </span>
            </div>
          </div>

          <div className="rounded-lg border-l border-zinc-800 p-2 pl-3 transition-colors hover:bg-zinc-900/70">
            <div className="flex items-baseline justify-between">
              <span className="text-xs font-medium text-zinc-400">Home</span>
              <span
                className={
                  "text-lg font-semibold tabular-nums " +
                  (homeValue ? "text-zinc-100" : "text-zinc-600")
                }
              >
                {homeValue ?? "—"}
              </span>
            </div>
          </div>
        </div>
      </div>

      {showModelCompare && (
        <div className="flex items-center justify-between text-xs text-zinc-500">
          <span className="flex items-center gap-1">
            <span className="h-1 w-1 rounded-full bg-blue-400" />
            <span>Model reference</span>
          </span>
          <span className="font-medium tabular-nums text-zinc-300">
            {modelAwayFormatted ?? "—"} / {modelHomeFormatted ?? "—"}
          </span>
        </div>
      )}
    </div>
  );
}

function InlineOddsRow({
  odds,
}: {
  odds: AggregatedOdds | null;
}) {
  if (!odds) {
    return (
      <div className="rounded-lg border border-dashed border-zinc-800/70 bg-zinc-900/40 px-3 py-2 text-xs text-zinc-500">
        Market snapshot unavailable
      </div>
    );
  }

  const moneylineHome = formatAmericanOdds(odds.moneyline.home);
  const moneylineAway = formatAmericanOdds(odds.moneyline.away);
  const spreadHome =
    odds.spreads.home.point === null || odds.spreads.home.point === undefined
      ? null
      : (odds.spreads.home.point > 0 ? "+" : "") + odds.spreads.home.point;
  const spreadAway =
    odds.spreads.away.point === null || odds.spreads.away.point === undefined
      ? null
      : (odds.spreads.away.point > 0 ? "+" : "") + odds.spreads.away.point;
  const spreadHomePrice = formatAmericanOdds(odds.spreads.home.price);
  const spreadAwayPrice = formatAmericanOdds(odds.spreads.away.price);

  const books =
    odds.moneyline.bookmakers.length > 0
      ? odds.moneyline.bookmakers
      : odds.spreads.bookmakers;
  const bookAttribution =
    books.length === 0
      ? null
      : books.length > 2
      ? `${books.slice(0, 2).join(" / ")} +${books.length - 2}`
      : books.join(" / ");

  return (
    <div className="rounded-xl border border-zinc-800/70 bg-zinc-950/60 p-4 text-xs">
      <div className="flex items-center justify-between text-[11px] uppercase tracking-wide text-zinc-500">
        <span title="Live lines from leading books">Market snapshot</span>
        {bookAttribution && (
          <span className="text-xs font-medium normal-case tracking-normal text-zinc-400">
            {bookAttribution}
          </span>
        )}
      </div>

      <div className="grid grid-cols-2 gap-4 pt-3">
        <div className="rounded-lg p-2 transition-colors hover:bg-zinc-900/70">
          <div className="flex items-baseline justify-between">
            <span className="text-xs font-medium text-zinc-400">Away</span>
            <span
              className={
                "text-base font-semibold tabular-nums " +
                (moneylineAway ? "text-white" : "text-zinc-600")
              }
            >
              {moneylineAway ?? "—"}
            </span>
          </div>
          <div className="mt-1 flex items-baseline justify-between text-sm">
            <span
              className="text-[11px] uppercase tracking-wide text-zinc-500"
              title="Point handicap to balance outcomes"
            >
              Spread
            </span>
            <div className="flex items-baseline gap-2">
              <span
                className={
                  "text-sm font-medium tabular-nums " +
                  (spreadAway ? "text-zinc-200" : "text-zinc-600")
                }
              >
                {spreadAway ?? "—"}
              </span>
              <span
                className={
                  "text-xs tabular-nums " +
                  (spreadAwayPrice ? "text-zinc-500" : "text-zinc-700")
                }
              >
                {spreadAwayPrice ?? "—"}
              </span>
            </div>
          </div>
        </div>

        <div className="rounded-lg border-l border-zinc-800 p-2 pl-3 transition-colors hover:bg-zinc-900/70">
          <div className="flex items-baseline justify-between">
            <span className="text-xs font-medium text-zinc-400">Home</span>
            <span
              className={
                "text-base font-semibold tabular-nums " +
                (moneylineHome ? "text-white" : "text-zinc-600")
              }
            >
              {moneylineHome ?? "—"}
            </span>
          </div>
          <div className="mt-1 flex items-baseline justify-between text-sm">
            <span
              className="text-[11px] uppercase tracking-wide text-zinc-500"
              title="Point handicap to balance outcomes"
            >
              Spread
            </span>
            <div className="flex items-baseline gap-2">
              <span
                className={
                  "text-sm font-medium tabular-nums " +
                  (spreadHome ? "text-zinc-200" : "text-zinc-600")
                }
              >
                {spreadHome ?? "—"}
              </span>
              <span
                className={
                  "text-xs tabular-nums " +
                  (spreadHomePrice ? "text-zinc-500" : "text-zinc-700")
                }
              >
                {spreadHomePrice ?? "—"}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function ModelSnapshotRow({
  modelSnapshot,
  awayTeam,
  homeTeam,
}: {
  modelSnapshot: { source: string; p_home_win: number; p_away_win: number } | null | undefined;
  awayTeam?: string;
  homeTeam?: string;
}) {
  if (!modelSnapshot) {
    return (
      <div className="rounded-lg border border-dashed border-zinc-800/70 bg-zinc-900/40 px-3 py-2 text-xs text-zinc-500">
        No AI estimate available
      </div>
    );
  }

  const homeProb = modelSnapshot.p_home_win * 100;
  const awayProb = modelSnapshot.p_away_win * 100;
  const favorite = homeProb >= awayProb ? "home" : "away";

  // Shorten team names for compact display
  const shortTeamName = (name: string | undefined) => {
    if (!name) return "";
    // For long names, take first word or first 3 letters
    const words = name.split(" ");
    if (words.length > 1) {
      // Multi-word: use first word (e.g., "Los Angeles" -> "LA", "Golden State" -> "Golden")
      return words[0];
    }
    // Single word: truncate if very long
    return name.length > 12 ? name.substring(0, 10) + "..." : name;
  };

  const awayShort = shortTeamName(awayTeam);
  const homeShort = shortTeamName(homeTeam);

  return (
    <div className="overflow-hidden rounded-xl border border-indigo-900/40 bg-gradient-to-br from-indigo-950/20 via-violet-950/10 to-zinc-950/40 p-4">
      {/* Header */}
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-base">🤖</span>
          <span className="text-xs font-medium uppercase tracking-wider text-indigo-400">
            AI Win Probability
          </span>
        </div>
        <span className="text-[10px] text-zinc-500">Model estimate</span>
      </div>

      {/* Horizontal probability split */}
      <div className="space-y-3">
        {/* Visual bar showing split */}
        <div className="relative h-10 overflow-hidden rounded-lg bg-zinc-900/60">
          {/* Away side (left) */}
          <div
            className="absolute left-0 top-0 h-full bg-gradient-to-r from-violet-500/30 to-violet-600/20 transition-all duration-500"
            style={{ width: `${awayProb}%` }}
          />
          {/* Home side (right) */}
          <div
            className="absolute right-0 top-0 h-full bg-gradient-to-l from-indigo-500/30 to-indigo-600/20 transition-all duration-500"
            style={{ width: `${homeProb}%` }}
          />

          {/* Overlaid text */}
          <div className="relative flex h-full items-center justify-between px-3 text-sm">
            <div className="flex items-center gap-2">
              {awayShort && (
                <span className={`text-xs font-medium ${favorite === "away" ? "text-violet-300" : "text-zinc-400"}`}>
                  {awayShort}
                </span>
              )}
              <span className={`text-lg font-bold tabular-nums ${favorite === "away" ? "text-white" : "text-zinc-300"}`}>
                {awayProb.toFixed(0)}%
              </span>
            </div>
            <div className="flex items-center gap-2">
              <span className={`text-lg font-bold tabular-nums ${favorite === "home" ? "text-white" : "text-zinc-300"}`}>
                {homeProb.toFixed(0)}%
              </span>
              {homeShort && (
                <span className={`text-xs font-medium ${favorite === "home" ? "text-indigo-300" : "text-zinc-400"}`}>
                  {homeShort}
                </span>
              )}
            </div>
          </div>
        </div>

        {/* Disclaimer */}
        <p className="text-center text-[10px] text-zinc-500">
          Model estimate · Not betting odds
        </p>
      </div>
    </div>
  );
}

export default function GamesPage() {
  const [events, setEvents] = useState<Event[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [oddsGames, setOddsGames] = useState<ApiGameOdds[]>([]);
  const [oddsLoaded, setOddsLoaded] = useState(false);
  const [seasonsMeta, setSeasonsMeta] = useState<Record<string, number[]>>({});

  // Season filter ("Season" dropdown) – default is "all"; we will explicitly move to latest only if desired
  const [selectedSport, setSelectedSport] = useState<SportFilterId>("all");
  const [yearFilter, setYearFilter] = useState<string>("all");

  // Team filter – defaults to "All teams"
  const [teamFilter, setTeamFilter] = useState<string>("all");

  // Optional date filter – starts as null ("All dates"), only applied when set
  const [dateFilter, setDateFilter] = useState<string | null>(null);

  const [showScrollTop, setShowScrollTop] = useState(false);

  // Load seasons metadata (per sport) from backend
  useEffect(() => {
    (async () => {
      try {
        const data = await api.seasonsMeta();
        setSeasonsMeta(data.seasons ?? {});
      } catch (err) {
        console.warn("Failed to load seasons meta", err);
        setSeasonsMeta({});
      }
    })();
  }, []);

  // ✅ "Jump to today" helper – sets the calendar date to today and also moves Season to current year
  const handleJumpToToday = () => {
    const today = getLocalISODate();
    setDateFilter(today);
    const year = today.slice(0, 4);
    setYearFilter(year);
  };

  // ✅ Date shift helper – move the current dateFilter backward/forward by N days
  // If no date is selected yet, we treat today as the starting point.
  const handleShiftDate = (deltaDays: number) => {
    // Start from the current filter date if set; otherwise from today (local)
    const baseStr = dateFilter ?? getLocalISODate();
    const parts = baseStr.split("-").map((p) => parseInt(p, 10));
    if (parts.length !== 3 || parts.some((n) => Number.isNaN(n))) return;

    const [year, month, day] = parts;
    const baseDate = new Date(year, month - 1, day); // local midnight
    baseDate.setDate(baseDate.getDate() + deltaDays);

    const shifted = getLocalISODate(baseDate);
    setDateFilter(shifted);
    // Keep the season in sync with the shifted date
    setYearFilter(String(baseDate.getFullYear()));
  };

  const scrollToTop = useCallback(() => {
    if (typeof window === "undefined") return;
    window.scrollTo({ top: 0, behavior: "smooth" });
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const onScroll = () => {
      setShowScrollTop(window.scrollY > 400);
    };

    window.addEventListener("scroll", onScroll);
    onScroll();

    return () => {
      window.removeEventListener("scroll", onScroll);
    };
  }, []);

  // 1) Fetch events + teams
  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        // log the outbound request params
        const requestParams = {
          sport: selectedSport === "all" ? undefined : selectedSport === 1 ? "NBA" : selectedSport === 2 ? "MLB" : selectedSport === 3 ? "NFL" : selectedSport === 4 ? "NHL" : selectedSport === 5 ? "UFC" : undefined,
          sport_id: selectedSport === "all" ? undefined : (selectedSport as number),
          date: dateFilter ?? undefined,
          season: yearFilter === "all" ? undefined : yearFilter,
          limit: 50000,
        };
        const [eventsRes, teamsRes] = await Promise.all([
          api.events({
            limit: requestParams.limit,
            sport: requestParams.sport,
            sport_id: requestParams.sport_id,
            date: requestParams.date,
            season: requestParams.season,
          }),
          api.teams(),
        ]);
        console.log("events fetch", {
          url: "/events",
          params: requestParams,
          first5: (eventsRes.items || []).slice(0, 5).map((e) => ({
            date: e.date,
            home: e.home_team ?? (e as any).home_team_name,
            away: e.away_team ?? (e as any).away_team_name,
            sport: e.sport_id,
            season: yearFilter,
          })),
          total: (eventsRes.items || []).length,
        });
        setEvents(eventsRes.items || []);
        setTeams(teamsRes.items || []);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load games";
        setError(message);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  // Fetch odds from local Next API (parquet-backed) for NBA/NFL/NHL/UFC
  useEffect(() => {
    const sportsToLoad =
      selectedSport === "all"
        ? ["nba", "nfl", "nhl", "ufc"]
        : selectedSport === 1
        ? ["nba"]
        : selectedSport === 3
        ? ["nfl"]
        : selectedSport === 4
        ? ["nhl"]
        : selectedSport === 5
        ? ["ufc"]
        : [];

    if (sportsToLoad.length === 0) {
      setOddsGames([]);
      setOddsLoaded(false);
      return;
    }

    (async () => {
      try {
        const responses = await Promise.all(
          sportsToLoad.map(async (sportKey) => {
            const query = new URLSearchParams({
              sport: sportKey,
              hours: String(ODDS_FETCH_HOURS),
            });
            const res = await fetch(`/api/odds?${query.toString()}`, {
              cache: "no-store",
            });
            if (!res.ok) {
              throw new Error(`Odds API failed for ${sportKey}: ${res.status}`);
            }
            const data = await res.json();
            if (process.env.NODE_ENV !== "production") {
              console.log(`Odds loaded (${sportKey}):`, {
                games: data.games?.length ?? 0,
                rows_loaded: data.meta?.rows_loaded,
                rows_filtered: data.meta?.rows_filtered,
                file_found: data.meta?.file_found,
              });
            }
            return data.games ?? [];
          }),
        );

        const merged = responses.flat();
        setOddsGames(merged);
      } catch (err) {
        console.error("Failed to load odds", err);
        setOddsGames([]);
      } finally {
        setOddsLoaded(true);
      }
    })();
  }, [selectedSport]);

  // 2) After seasons metadata or events load, default Season filter to latest year ("current season")
  useEffect(() => {
    if (events.length === 0) return;
    if (yearFilter !== "all") return;

    // Prefer meta seasons for selected sport (or all)
    const sportKey = selectedSport === "all" ? null : String(selectedSport);
    const metaYears =
      sportKey && seasonsMeta[sportKey] ? seasonsMeta[sportKey].map(String) : [];

    const years =
      metaYears.length > 0
        ? metaYears.sort()
        : Array.from(
            new Set(
              events
                .map((e) => getYearFromDate(e.date))
                .filter((y): y is string => !!y),
            ),
          ).sort();

    const latest = years[years.length - 1];
    if (latest) {
      setYearFilter(latest);
    }
  }, [events, yearFilter, seasonsMeta, selectedSport]);

  // Log all unique NFL team labels based on events
  useEffect(() => {
    if (!events.length) return;

    // Only NFL events
    const nflEvents = events.filter((e) => e.sport_id === 3);

    const names = new Set<string>();

    for (const e of nflEvents) {
      const home =
        (e as any).home_team_name ??
        (e as any).home_team ??
        null;
      const away =
        (e as any).away_team_name ??
        (e as any).away_team ??
        null;

      if (home) names.add(String(home));
      if (away) names.add(String(away));
    }

    console.log("NFL team labels in events:", Array.from(names));
  }, [events]);

  // Debug: count events per sport_id and sample NHL rows
  useEffect(() => {
    if (!events.length) return;

    const bySport: Record<number, number> = {};
    for (const e of events) {
      const key = Number(e.sport_id ?? 0);
      bySport[key] = (bySport[key] ?? 0) + 1;
    }

    console.log("Events by sport_id from API:", bySport);

    const nhlSample = events
      .filter((e) => e.sport_id === 4)
      .slice(0, 5)
      .map((e) => ({
        event_id: e.event_id,
        date: e.date,
        sport_id: e.sport_id,
        home_team_id: e.home_team_id,
        away_team_id: e.away_team_id,
      }));

    console.log("Sample NHL events from API (sport_id = 4):", nhlSample);
  }, [events]);

  // Team lookup using shared helpers
  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);

  // Index odds by normalized matchup for quick lookup
  const oddsIndex = useMemo(() => {
    const map = new Map<string, ApiGameOdds[]>();
    for (const game of oddsGames) {
      const key = buildGameKey(game.home_team, game.away_team);
      const existing = map.get(key) ?? [];
      existing.push(game);
      map.set(key, existing);
    }
    return map;
  }, [oddsGames]);

  useEffect(() => {
    if (!oddsGames.length || !events.length) return;
    if (process.env.NODE_ENV === "production") return;
    const nbaEvents = events.filter((e) => e.sport_id === 1);
    const nflEvents = events.filter((e) => e.sport_id === 3);
    const nhlEvents = events.filter((e) => e.sport_id === 4);
    const ufcEvents = events.filter((e) => e.sport_id === 5);

    const countMatches = (items: Event[], label: string) => {
      let matched = 0;
      for (const e of items) {
        const homeName = getTeamLabel(e, "home", teamsById);
        const awayName = getTeamLabel(e, "away", teamsById);
        const game = findMatchingOddsGame(
          oddsIndex,
          homeName,
          awayName,
          e.date,
          (e as any).start_time ?? (e as any).start_et ?? null,
        );
        if (game) matched += 1;
      }
      console.log(`Odds matching: matched ${matched}/${items.length} ${label} games`);
    };

    countMatches(nbaEvents, "NBA");
    countMatches(nflEvents, "NFL");
    countMatches(nhlEvents, "NHL");
    countMatches(ufcEvents, "UFC");
  }, [oddsGames, events, teamsById, oddsIndex]);

  // Sport filters (UI only)
  const sportFilters: { id: SportFilterId; label: string }[] = SPORT_FILTERS;

  // Year options derived from events – for the Season dropdown
  const yearOptions = useMemo(() => {
    // Prefer backend-provided seasons metadata; fallback to derived from events
    const sportKey = selectedSport === "all" ? null : String(selectedSport);
    const metaYears =
      sportKey && seasonsMeta[sportKey] ? seasonsMeta[sportKey].map(String) : [];

    if (metaYears.length > 0) {
      const sorted = [...metaYears].sort((a, b) => Number(a) - Number(b));
      console.log("yearOptions (meta)", { selectedSport, sorted });
      return sorted;
    }

    const relevantEvents =
      selectedSport === "all"
        ? events
        : events.filter((e) => e.sport_id === selectedSport);

    const years = new Set<string>();
    for (const e of relevantEvents) {
      const y = getYearFromDate(e.date);
      if (y) years.add(y);
    }
    const sorted = Array.from(years).sort((a, b) => Number(a) - Number(b));
    console.log("yearOptions (events fallback)", { selectedSport, sorted });
    return sorted;
  }, [events, selectedSport, seasonsMeta]);

  // Ensure the yearFilter stays valid for the selected sport; if invalid, reset to latest or all.
  useEffect(() => {
    if (yearFilter !== "all" && !yearOptions.includes(yearFilter)) {
      console.log("yearFilter not in options, resetting to all", { yearFilter, yearOptions });
      setYearFilter("all");
      return;
    }

    // If selecting a specific sport and currently "all", move to latest available season
    if (selectedSport !== "all" && yearFilter === "all" && yearOptions.length > 0) {
      const latest = yearOptions[yearOptions.length - 1];
      console.log("setting yearFilter to latest for sport", { selectedSport, latest });
      setYearFilter(latest);
    }
  }, [selectedSport, yearFilter, yearOptions]);

  // Team options derived from teams – filtered by sport
  const teamOptions = useMemo(() => {
    if (!teams.length) return [];

    const filteredTeams =
      selectedSport === "all"
        ? teams
        : teams.filter((t) => t.sport_id === selectedSport);

    const labels = new Set<string>();
    for (const t of filteredTeams) {
      if (t.name) {
        labels.add(t.name);
      }
    }
    return Array.from(labels).sort();
  }, [teams, selectedSport]);

  // When sport changes, reset team filter so you don't get "empty" state from stale team
  useEffect(() => {
    setTeamFilter("all");
  }, [selectedSport]);

  // When the sport filter changes, ensure the season/year filter points to an available year
  // for that sport so NHL (and other leagues) surface immediately.
  useEffect(() => {
    // When switching sports, default to "all" seasons so we don't get trapped on earliest season.
    setYearFilter("all");
  }, [selectedSport]);

  // ================================
  // Composed filtering logic (order)
  // 1) Season (yearFilter)
  // 2) Sport (selectedSport)
  // 3) Team (teamFilter)
  // 4) Optional Date (dateFilter)
  // ================================
  const visibleEvents = useMemo(() => {
    let filtered = events;

    console.log("filtering events", {
      before: events.length,
      selectedSport,
      yearFilter,
      dateFilter,
      teamFilter,
    });

    // sport filter
    const beforeSport = filtered.length;
    // 1) Sport filter (already applied server-side but keep for safety)
    if (selectedSport !== "all") {
      filtered = filtered.filter((e) => e.sport_id === selectedSport);
    }
    console.log("after sport filter", { before: beforeSport, after: filtered.length });

    const beforeSeason = filtered.length;
    // 2) Season filter (keep in sync with server response)
    if (yearFilter !== "all") {
      filtered = filtered.filter((e) => getYearFromDate(e.date) === yearFilter);
    }
    console.log("after season filter", { before: beforeSeason, after: filtered.length });

    const beforeTeam = filtered.length;
    // 3) Team filter
    if (teamFilter !== "all") {
      filtered = filtered.filter((e) => {
        const homeName = getTeamLabel(e, "home", teamsById);
        const awayName = getTeamLabel(e, "away", teamsById);
        return homeName === teamFilter || awayName === teamFilter;
      });
    }
    console.log("after team filter", { before: beforeTeam, after: filtered.length });

    const beforeDate = filtered.length;
    // 4) Optional Date filter – only applied if user selected a date (already applied server-side)
    if (dateFilter) {
      filtered = filtered.filter((e) => {
        const eventDate = (e.date ?? "").slice(0, 10);
        return eventDate === dateFilter;
      });
    }
    console.log("after date filter", { before: beforeDate, after: filtered.length });

    console.log("filtering events result", {
      after: filtered.length,
      sample: filtered.slice(0, 3),
    });

    // 5) Remove bogus NFL rows that would render as "TBD @ TBD"
    filtered = filtered.filter((e) => {
      if (e.sport_id !== 3) return true;

      // Use the robust team label helper
      let homeTeamName: string = getTeamLabel(e, "home", teamsById);
      let awayTeamName: string = getTeamLabel(e, "away", teamsById);

      const expandNflName = (name: string): string => {
        const key = name.toUpperCase().trim();
        return NFL_TEAM_FULL_NAME_BY_ABBR[key] ?? name;
      };

      homeTeamName = expandNflName(homeTeamName);
      awayTeamName = expandNflName(awayTeamName);

      const derived = deriveNflNamesFromEventId(
        (e as any).event_id ?? e.event_id,
      );
      if (homeTeamName === "TBD" && derived.home) {
        homeTeamName = derived.home;
      }
      if (awayTeamName === "TBD" && derived.away) {
        awayTeamName = derived.away;
      }

      const isNflTbdMatchup =
        (homeTeamName === "TBD") &&
        (awayTeamName === "TBD");

      return !isNflTbdMatchup;
    });

    // Sort to show games with model predictions first, then by date descending
    filtered.sort((a, b) => {
      const aHasSnapshot = !!a.model_snapshot;
      const bHasSnapshot = !!b.model_snapshot;

      // Prioritize games with model_snapshot
      if (aHasSnapshot && !bHasSnapshot) return -1;
      if (!aHasSnapshot && bHasSnapshot) return 1;

      // If both have or don't have snapshot, sort by date descending (newest first)
      return b.date.localeCompare(a.date);
    });

    return filtered;
  }, [events, yearFilter, selectedSport, teamFilter, dateFilter, teamsById]);

  const visibleCount = visibleEvents.length;
  const isTeamDisabled = selectedSport === "all";
  const teamLeagueLabel =
    selectedSport === "all"
      ? "All leagues"
      : getSportLabelFromFilterId(selectedSport);

  // Limit how many game cards we render at once to keep the page responsive
  const MAX_EVENTS_TO_RENDER = 400;
  const eventsToRender = visibleEvents.slice(0, MAX_EVENTS_TO_RENDER);
  const isTruncated = visibleEvents.length > MAX_EVENTS_TO_RENDER;

  return (
    <main className="min-h-screen bg-black px-4 pb-12 pt-6 text-white">
      <div className="mx-auto w-full max-w-7xl space-y-6">
        {/* ========================= */}
        {/* 1. PAGE HEADER            */}
        {/* ========================= */}
        <section className="px-1">
          <header className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h1 className="text-3xl font-bold tracking-tight text-white sm:text-4xl">
                Games
              </h1>
              <p className="mt-1.5 max-w-2xl text-sm text-zinc-400">
                Live lines, model predictions, and historical results across all major leagues
              </p>
            </div>
          </header>
        </section>

        {/* ========================= */}
        {/* 2. FILTER BAR             */}
        {/* ========================= */}
        <section className="overflow-hidden rounded-2xl border border-zinc-900/50 bg-gradient-to-br from-zinc-950/40 via-zinc-950/60 to-zinc-950/40 backdrop-blur-sm">
          <div className="space-y-4 p-5">
            {/* Sport pills - Primary filter */}
            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between">
                <h3 className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                  League
                </h3>
                {loading && (
                  <span className="text-xs text-zinc-500">Loading…</span>
                )}
                {error && (
                  <span className="text-xs text-red-400/90">{error}</span>
                )}
              </div>
              <div className="flex gap-2 overflow-x-auto pb-1 scrollbar-thin scrollbar-track-transparent scrollbar-thumb-zinc-800">
                {sportFilters.map((f) => (
                  <button
                    key={f.id}
                    type="button"
                    onClick={() => setSelectedSport(f.id)}
                    aria-pressed={selectedSport === f.id}
                    className={
                      "group relative shrink-0 overflow-hidden rounded-xl border px-4 py-2.5 text-sm font-medium shadow-lg transition-all duration-200 " +
                      (selectedSport === f.id
                        ? "border-blue-500/50 bg-gradient-to-br from-blue-500/20 to-blue-600/10 text-blue-100 shadow-blue-500/20"
                        : "border-zinc-800/80 bg-zinc-900/40 text-zinc-400 shadow-black/20 hover:border-zinc-700 hover:bg-zinc-900/60 hover:text-zinc-200 active:scale-[0.98]")
                    }
                  >
                    {selectedSport === f.id && (
                      <div className="absolute inset-0 bg-gradient-to-br from-blue-500/10 to-transparent" />
                    )}
                    <span className="relative inline-flex items-center gap-2">
                      {f.id !== "all" && (
                        <span className="text-base">
                          {sportIconFromId(f.id as number)}
                        </span>
                      )}
                      <span>{f.label}</span>
                    </span>
                  </button>
                ))}
              </div>
            </div>

            {/* Secondary filters: Date + Season + Team */}
            <div className="grid gap-4 border-t border-zinc-800/50 pt-4 sm:grid-cols-3">
              {/* Date navigation */}
              <div className="space-y-2">
                <label className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                  Date
                </label>
                <div className="flex items-center gap-1.5">
                  <button
                    type="button"
                    onClick={() => handleShiftDate(-1)}
                    className="flex h-9 w-9 items-center justify-center rounded-lg border border-zinc-800 bg-zinc-900/60 text-sm text-zinc-400 shadow-sm transition-all duration-150 hover:border-zinc-700 hover:bg-zinc-800/80 hover:text-zinc-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50 active:scale-95"
                    aria-label="Previous day"
                  >
                    ←
                  </button>
                  <input
                    type="date"
                    value={dateFilter ?? ""}
                    onChange={(e) =>
                      setDateFilter(
                        e.target.value === "" ? null : e.target.value,
                      )
                    }
                    className="flex-1 rounded-lg border border-zinc-800 bg-zinc-900/60 px-3 py-2 text-sm text-zinc-200 shadow-sm transition-all duration-150 focus:border-blue-500/50 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                  />
                  <button
                    type="button"
                    onClick={() => handleShiftDate(1)}
                    className="flex h-9 w-9 items-center justify-center rounded-lg border border-zinc-800 bg-zinc-900/60 text-sm text-zinc-400 shadow-sm transition-all duration-150 hover:border-zinc-700 hover:bg-zinc-800/80 hover:text-zinc-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50 active:scale-95"
                    aria-label="Next day"
                  >
                    →
                  </button>
                  <button
                    type="button"
                    onClick={handleJumpToToday}
                    title="Jump to today"
                    className="rounded-lg border border-zinc-800 bg-zinc-900/60 px-3 py-2 text-xs font-medium text-zinc-400 shadow-sm transition-all duration-150 hover:border-zinc-700 hover:bg-zinc-800/80 hover:text-zinc-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50 active:scale-95"
                  >
                    Today
                  </button>
                </div>
              </div>

              {/* Season selector */}
              <div className="space-y-2">
                <label className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                  Season
                </label>
                <div className="relative">
                  <select
                    value={yearFilter}
                    onChange={(e) => {
                      const value = e.target.value;
                      setYearFilter(value);
                      setDateFilter(null);
                    }}
                    className="w-full appearance-none rounded-lg border border-zinc-800 bg-zinc-900/60 px-3 py-2 pr-9 text-sm text-zinc-200 shadow-sm transition-all duration-150 focus:border-blue-500/50 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                  >
                    <option value="all">All seasons</option>
                    {yearOptions.map((y) => (
                      <option key={y} value={y}>
                        {y} season
                      </option>
                    ))}
                  </select>
                  <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-zinc-500">
                    ▼
                  </span>
                </div>
              </div>

              {/* Team selector */}
              <div className="space-y-2">
                <label className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                  Team
                  {!isTeamDisabled && (
                    <span className="ml-1.5 text-[10px] font-normal text-zinc-600">
                      ({teamLeagueLabel})
                    </span>
                  )}
                </label>
                <div className="relative">
                  <select
                    value={teamFilter}
                    onChange={(e) => setTeamFilter(e.target.value)}
                    disabled={isTeamDisabled}
                    className={
                      "w-full appearance-none rounded-lg border px-3 py-2 pr-9 text-sm shadow-sm transition-all duration-150 " +
                      (isTeamDisabled
                        ? "cursor-not-allowed border-zinc-800/50 bg-zinc-900/30 text-zinc-600"
                        : "border-zinc-800 bg-zinc-900/60 text-zinc-200 focus:border-blue-500/50 focus:outline-none focus:ring-2 focus:ring-blue-500/20")
                    }
                  >
                    <option value="all">
                      {isTeamDisabled
                        ? "Select a league first"
                        : "All teams"}
                    </option>
                    {!isTeamDisabled &&
                      teamOptions.map((name) => (
                        <option key={name} value={name}>
                          {name}
                        </option>
                      ))}
                  </select>
                  <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-zinc-500">
                    ▼
                  </span>
                </div>
              </div>
            </div>

            {/* Empty-state helper message */}
            {!loading && !error && visibleEvents.length === 0 && (
              <div className="rounded-lg border border-zinc-800/50 bg-zinc-900/40 px-4 py-3 text-sm">
                <p className="font-medium text-zinc-300">No games found</p>
                <p className="mt-1 text-xs text-zinc-500">
                  Try adjusting your filters or selecting a different date range
                </p>
              </div>
            )}
          </div>
        </section>

        {/* ========================= */}
        {/* 3. SUMMARY BAR            */}
        {/* ========================= */}
        <SummaryBar
          selectedSport={selectedSport}
          visibleCount={visibleCount}
          dateFilter={dateFilter}
          yearFilter={yearFilter}
        />

        {/* ========================= */}
        {/* 4. GAME CARDS GRID        */}
        {/* ========================= */}
        <section className="space-y-5">
          {isTruncated && (
            <div className="rounded-lg border border-amber-900/30 bg-gradient-to-r from-amber-950/20 to-amber-900/10 px-4 py-3">
              <div className="flex items-start gap-3">
                <span className="text-lg">⚡</span>
                <div className="flex-1">
                  <p className="text-sm font-medium text-amber-400/90">
                    Showing first {MAX_EVENTS_TO_RENDER.toLocaleString()} games
                  </p>
                  <p className="mt-1 text-xs text-amber-600/80">
                    Refine your filters to narrow results or scroll down to load more
                  </p>
                </div>
              </div>
            </div>
          )}
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-2">
            {eventsToRender.map((e) => {
              // Combine *_score with *_pts so 2025 games show as Final
              const homeScoreRaw =
                (e as any).home_score ?? (e as any).home_pts ?? null;
              const awayScoreRaw =
                (e as any).away_score ?? (e as any).away_pts ?? null;

              const homeScore =
                homeScoreRaw !== null ? Number(homeScoreRaw) : null;
              const awayScore =
                awayScoreRaw !== null ? Number(awayScoreRaw) : null;

              const hasScores =
                homeScore !== null && awayScore !== null && !Number.isNaN(homeScore) &&
                !Number.isNaN(awayScore);

              const statusNorm = (e.status ?? "").toString().toLowerCase();
              const isLive = statusNorm === "in_progress" || statusNorm === "live";
              const isFinal = statusNorm === "final" || (hasScores && !["scheduled", "pre"].includes(statusNorm));

              // Determine homeWin from field or from scores
              const homeWin =
                isFinal && e.home_win != null
                  ? Boolean(e.home_win)
                  : isFinal && hasScores
                  ? homeScore! > awayScore!
                  : null;

              const homeIsWinner = isFinal && homeWin === true;
              const awayIsWinner = isFinal && homeWin === false;

              // Prefer explicit team name fields from the event (needed for NFL),
              // then fall back to the shared teams lookup, then finally "TBD".
              // Use robust team label helper
              let homeTeamName: string = getTeamLabel(e, "home", teamsById);
              let awayTeamName: string = getTeamLabel(e, "away", teamsById);

              // For NFL, manually map common abbreviations (JAX, TEN, NO, TB, etc.)
              // to full team names so the UI always shows the full franchise name.
              if (e.sport_id === 3) {
                const expandNflName = (name: string): string => {
                  const key = name.toUpperCase().trim();
                  return NFL_TEAM_FULL_NAME_BY_ABBR[key] ?? name;
                };

                // First try to expand whatever names we already have (from teams table)
                homeTeamName = expandNflName(homeTeamName);
                awayTeamName = expandNflName(awayTeamName);

                // As a backup, if we still did not get a full name, try to derive
                // abbreviations from the event_id pattern like "2019_01_ARI_PHI".
                const derived = deriveNflNamesFromEventId(
                  (e as any).event_id ?? e.event_id,
                );
                if (derived.home) {
                  homeTeamName = derived.home;
                }
                if (derived.away) {
                  awayTeamName = derived.away;
                }
              }

              // For NHL, manually map common abbreviations (CGY, VAN, TOR, MTL, etc.)
              // to full team names so the UI shows "Calgary Flames" not "CGY".
              if (e.sport_id === 4) {
                const expandNhlName = (name: string): string => {
                  const key = name.toUpperCase().trim();
                  return NHL_TEAM_NAMES[key] ?? name;
                };

                // Expand whatever names we got from getTeamLabel
                homeTeamName = expandNhlName(homeTeamName);
                awayTeamName = expandNhlName(awayTeamName);
              }

              // Already strings from getTeamLabel
              const finalHomeTeamName = homeTeamName;
              const finalAwayTeamName = awayTeamName;

              const eventStartTimeStr =
                (e as any).start_time ?? (e as any).start_et ?? null;

              const sportSupportsOdds =
                e.sport_id === 1 || e.sport_id === 3 || e.sport_id === 4 || e.sport_id === 5;

              const matchedOddsGame =
                sportSupportsOdds
                  ? findMatchingOddsGame(
                      oddsIndex,
                      finalHomeTeamName,
                      finalAwayTeamName,
                      e.date,
                      eventStartTimeStr,
                    )
                  : null;

              const aggregatedOdds =
                matchedOddsGame && !isFinal && !isLive
                  ? aggregateGameOdds(
                      matchedOddsGame,
                      finalHomeTeamName,
                      finalAwayTeamName,
                    )
                  : null;

              // --- Odds (for scheduled games) ---
              const modelHomeOddsRaw =
                (e as any).model_home_american_odds ?? null;
              const modelAwayOddsRaw =
                (e as any).model_away_american_odds ?? null;

              const bookHomeOddsRaw =
                (e as any).sportsbook_home_american_odds ?? null;
              const bookAwayOddsRaw =
                (e as any).sportsbook_away_american_odds ?? null;

              const hasSportsbookOdds =
                bookHomeOddsRaw !== null && bookAwayOddsRaw !== null;

              const homeOddsToShow =
                aggregatedOdds?.moneyline.home ??
                (hasSportsbookOdds ? bookHomeOddsRaw : modelHomeOddsRaw);
              const awayOddsToShow =
                aggregatedOdds?.moneyline.away ??
                (hasSportsbookOdds ? bookAwayOddsRaw : modelAwayOddsRaw);

              const homeOddsFormatted = formatAmericanOdds(homeOddsToShow);
              const awayOddsFormatted = formatAmericanOdds(awayOddsToShow);

              const status = getEventStatus(e, isFinal);

              const statusLabel =
                status === "final"
                  ? "Final"
                  : status === "live"
                  ? "Live"
                  : "Upcoming";

              const oddsLabel = isFinal || isLive
                ? null
                : aggregatedOdds
                ? "Sportsbook odds"
                : hasSportsbookOdds
                ? "Sportsbook odds"
                : "Model odds";

              const modelHomeFormatted =
                formatAmericanOdds(modelHomeOddsRaw);
              const modelAwayFormatted =
                formatAmericanOdds(modelAwayOddsRaw);

              // 🚫 Hide bogus rows where we never resolved either team.
              // These show up as "TBD @ TBD" and likely aren't real games.
              const isNflTbdMatchup =
                e.sport_id === 3 &&
                (!finalHomeTeamName || finalHomeTeamName === "TBD") &&
                (!finalAwayTeamName || finalAwayTeamName === "TBD");

              const isNbaTbdMatchup =
                e.sport_id === 1 &&
                (!finalHomeTeamName || finalHomeTeamName === "TBD") &&
                (!finalAwayTeamName || finalAwayTeamName === "TBD");

              if (isNflTbdMatchup || isNbaTbdMatchup) {
                // Skip rendering this card entirely
                return null;
              }

              // Use composite key to avoid duplicates when event_id repeats
              const cardKey = `${e.event_id}-${e.date}-${e.home_team}-${e.away_team}-${e.sport_id}`;

              return (
                <Link
                  key={cardKey}
                  href={`/games/${e.event_id}`}
                  prefetch={false}
                  className="group relative flex flex-col overflow-hidden rounded-xl border border-zinc-800/60 bg-gradient-to-br from-zinc-900/40 to-zinc-950/60 shadow-xl shadow-black/40 backdrop-blur-sm transition-all duration-300 hover:border-blue-500/50 hover:shadow-2xl hover:shadow-blue-500/10 active:scale-[0.99]"
                >
                  {/* Subtle top gradient accent */}
                  <div className="pointer-events-none absolute left-0 top-0 h-px w-full bg-gradient-to-r from-transparent via-zinc-600/30 to-transparent" />

                  {/* Status indicator stripe */}
                  <div className={
                    "absolute left-0 top-0 h-full w-1 transition-all duration-300 " +
                    (status === "final"
                      ? "bg-gradient-to-b from-emerald-500/60 to-emerald-600/20"
                      : status === "live"
                      ? "bg-gradient-to-b from-red-500/80 to-red-600/30 animate-pulse"
                      : "bg-gradient-to-b from-blue-500/40 to-blue-600/10")
                  } />

                  <div className="flex flex-col gap-4 p-5">
                    {/* Header: League, Status, Date */}
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex items-center gap-2">
                        <span className="text-lg">
                          {sportIconFromId(e.sport_id)}
                        </span>
                        <span className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                          {sportLabelFromId(e.sport_id)}
                        </span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-zinc-500">
                          {formatDateWithTime(e.date, e.start_time) ?? e.date}
                        </span>
                        <span
                          className={
                            "rounded-md px-2 py-1 text-[10px] font-semibold uppercase tracking-wider " +
                            (status === "final"
                              ? "bg-emerald-500/15 text-emerald-400"
                              : status === "live"
                              ? "bg-red-500/15 text-red-400"
                              : "bg-zinc-800/60 text-zinc-400")
                          }
                        >
                          {status === "live" && (
                            <span className="mr-1 inline-flex h-1.5 w-1.5 animate-pulse rounded-full bg-red-400" />
                          )}
                          {statusLabel}
                        </span>
                      </div>
                    </div>

                    {/* Matchup */}
                    <div className="space-y-1">
                      <h2 className="text-lg font-bold leading-tight text-white">
                        {finalAwayTeamName}
                      </h2>
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-medium uppercase tracking-wider text-zinc-600">
                          {e.sport_id === 5 ? "vs" : "at"}
                        </span>
                      </div>
                      <h2 className="text-lg font-bold leading-tight text-white">
                        {finalHomeTeamName}
                      </h2>
                    </div>

                    {/* Value block – final vs scheduled */}
                    {e.sport_id === 5 && isFinal ? (
                      <UfcResultBlock
                        homeTeam={finalHomeTeamName}
                        awayTeam={finalAwayTeamName}
                        homeWin={homeWin}
                        event={e}
                      />
                    ) : (isFinal || isLive) && hasScores && homeScore !== null && awayScore !== null ? (
                      <FinalScoreBlock
                        homeTeam={finalHomeTeamName}
                        awayTeam={finalAwayTeamName}
                        homeScore={homeScore}
                        awayScore={awayScore}
                        homeIsWinner={homeIsWinner}
                        awayIsWinner={awayIsWinner}
                        homeWin={homeWin}
                      />
                    ) : (
                      <>
                        <OddsBlock
                          homeValue={homeOddsFormatted}
                          awayValue={awayOddsFormatted}
                          oddsLabel={oddsLabel}
                          modelHomeFormatted={modelHomeFormatted}
                          modelAwayFormatted={modelAwayFormatted}
                        />
                        {sportSupportsOdds && oddsLoaded && (
                          <div className="pt-2">
                            <InlineOddsRow odds={aggregatedOdds ?? null} />
                          </div>
                        )}
                        {e.model_snapshot && (
                          <div className="pt-2">
                            <ModelSnapshotRow
                              modelSnapshot={e.model_snapshot}
                              awayTeam={awayTeamName}
                              homeTeam={homeTeamName}
                            />
                          </div>
                        )}
                      </>
                    )}

                    {/* Footer: Model info + Actions */}
                    <div className="flex items-center justify-between border-t border-zinc-800/40 pt-4">
                      <div className="flex items-center gap-1.5">
                        <span className="h-1.5 w-1.5 rounded-full bg-blue-500/70" />
                        <span className="font-mono text-[10px] text-zinc-500">
                          logreg_v1
                        </span>
                      </div>
                      <div className="flex items-center gap-2">
                        <div className="flex items-center gap-1 rounded-lg bg-zinc-800/40 px-2.5 py-1.5 text-[11px] font-medium text-zinc-400 transition-colors group-hover:bg-blue-500/10 group-hover:text-blue-400">
                          <span className="text-sm">📊</span>
                          <span>Stats</span>
                        </div>
                        <div className="flex items-center gap-1 rounded-lg bg-zinc-800/40 px-2.5 py-1.5 text-[11px] font-medium text-zinc-400 transition-colors group-hover:bg-blue-500/10 group-hover:text-blue-400">
                          <span className="text-sm">✨</span>
                          <span>Analysis</span>
                        </div>
                      </div>
                    </div>
                  </div>
                </Link>
              );
            })}
          </div>
        </section>
      </div>

      {showScrollTop && (
        <button
          type="button"
          onClick={scrollToTop}
          className="fixed bottom-8 right-8 inline-flex h-12 w-12 items-center justify-center rounded-xl border border-zinc-800/60 bg-gradient-to-br from-zinc-900/90 to-zinc-950/90 text-lg text-zinc-300 shadow-2xl shadow-black/60 backdrop-blur-md transition-all duration-300 hover:border-blue-500/50 hover:bg-gradient-to-br hover:from-blue-500/20 hover:to-blue-600/10 hover:text-blue-100 hover:shadow-blue-500/20 active:scale-95"
          aria-label="Back to top"
        >
          ↑
        </button>
      )}
    </main>
  );
}
