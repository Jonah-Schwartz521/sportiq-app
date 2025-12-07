"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";
import { TeamValueBadge } from "@/lib/logos";

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

// --- Date / year helpers ---
function getYearFromDate(dateStr: string | null | undefined): string | null {
  if (!dateStr) return null;
  // assuming "YYYY-MM-DD" shape
  return dateStr.slice(0, 4);
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

// UI-only status helper, so we can style FINAL / LIVE / SCHEDULED
function getEventStatus(
  e: Event,
  isFinal: boolean,
): "final" | "scheduled" | "live" {
  if (e.status === "in_progress" || e.status === "live") return "live";
  if (isFinal || e.status === "final") return "final";
  return "scheduled";
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
    <section className="mt-1 rounded-2xl border border-zinc-900/80 bg-gradient-to-r from-zinc-950/90 via-black to-zinc-950/90 px-4 py-2.5 text-xs text-zinc-300 shadow-sm shadow-black/40">
      <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] text-zinc-300/90">
            <span className="h-1.5 w-1.5 rounded-full bg-blue-400/80" />
            {sportLabel}
          </span>
          <span className="text-[11px] text-zinc-400">
            <span className="font-semibold text-zinc-100">
              {visibleCount}
            </span>{" "}
            game{visibleCount === 1 ? "" : "s"}
          </span>
        </div>
        <span className="text-[11px] text-zinc-500">
          {dateLabel ?? ""}
        </span>
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
    <div className="mt-3 space-y-2 pl-1">
      {/* Away row */}
      <div
        className={
          "flex items-center justify-between gap-3 rounded-xl border px-3 py-2 transition-all duration-150 " +
          (awayIsWinner
            ? "border-emerald-500/70 bg-emerald-500/10 shadow-sm shadow-emerald-500/30"
            : "border-zinc-800 bg-zinc-950/80")
        }
      >
        <div className="flex flex-col">
          <span className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
            Away
          </span>
          <span className="text-sm font-medium text-zinc-50">
            {awayTeam}
          </span>
        </div>
        <span className="text-2xl font-semibold text-zinc-50">
          {awayScore}
        </span>
      </div>

      {/* Home row */}
      <div
        className={
          "flex items-center justify-between gap-3 rounded-xl border px-3 py-2 transition-all duration-150 " +
          (homeIsWinner
            ? "border-emerald-500/70 bg-emerald-500/10 shadow-sm shadow-emerald-500/30"
            : "border-zinc-800 bg-zinc-950/80")
        }
      >
        <div className="flex flex-col">
          <span className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
            Home
          </span>
          <span className="text-sm font-medium text-zinc-50">
            {homeTeam}
          </span>
        </div>
        <span className="text-2xl font-semibold text-zinc-50">
          {homeScore}
        </span>
      </div>

      {homeWin !== null && (
        <div className="flex justify-end">
          <span className="inline-flex items-center gap-1 rounded-full border border-zinc-700/80 bg-zinc-900/80 px-2.5 py-0.5 text-[10px] uppercase tracking-[0.16em] text-zinc-200">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
            {homeWin ? "Home team won" : "Away team won"}
          </span>
        </div>
      )}
    </div>
  );
}

// =========================
// Subcomponent: Odds block
// =========================
interface OddsBlockProps {
  homeTeam: string;
  awayTeam: string;
  homeValue: string | null;
  awayValue: string | null;
  oddsLabel: string | null;
  modelHomeFormatted: string | null;
  modelAwayFormatted: string | null;
}

function OddsBlock({
  homeTeam,
  awayTeam,
  homeValue,
  awayValue,
  oddsLabel,
  modelHomeFormatted,
  modelAwayFormatted,
}: OddsBlockProps) {
  const showValueRow = homeValue !== null || awayValue !== null;

  if (!showValueRow) return null;

  return (
    <div className="mt-3 space-y-2 pl-1">
      {/* Away odds row */}
      <div className="flex items-center justify-between gap-3">
        <button
          type="button"
          className="flex-1 rounded-full border border-zinc-700 bg-zinc-950/80 px-1 py-0.5 text-left text-xs text-zinc-100 shadow-sm shadow-black/40 transition-all duration-150 hover:border-blue-400 hover:bg-blue-500/10 hover:shadow-blue-500/30 active:scale-[0.97]"
        >
          <TeamValueBadge
            teamName={awayTeam}
            value={awayValue}
            variant="odds"
          />
        </button>
      </div>

      {/* Home odds row */}
      <div className="flex items-center justify-between gap-3">
        <button
          type="button"
          className="flex-1 rounded-full border border-zinc-700 bg-zinc-950/80 px-1 py-0.5 text-left text-xs text-zinc-100 shadow-sm shadow-black/40 transition-all duration-150 hover:border-blue-400 hover:bg-blue-500/10 hover:shadow-blue-500/30 active:scale-[0.97]"
        >
          <TeamValueBadge
            teamName={homeTeam}
            value={homeValue}
            variant="odds"
          />
        </button>
      </div>

      <div className="flex flex-col items-end gap-1">
        {oddsLabel && (homeValue || awayValue) && (
          <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
            {oddsLabel}
          </span>
        )}
        {/* Tiny secondary line to show model odds when sportsbook odds are present */}
        {oddsLabel === "Sportsbook odds" &&
          (modelHomeFormatted || modelAwayFormatted) && (
            <span className="text-[10px] text-zinc-500">
              Model: {modelAwayFormatted ?? "—"} /{" "}
              {modelHomeFormatted ?? "—"}
            </span>
          )}
      </div>
    </div>
  );
}

export default function GamesPage() {
  const [events, setEvents] = useState<Event[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Season filter ("Season" dropdown) – default will be set to latest once events load
  const [selectedSport, setSelectedSport] = useState<SportFilterId>("all");
  const [yearFilter, setYearFilter] = useState<string>("all");

  // Team filter – defaults to "All teams"
  const [teamFilter, setTeamFilter] = useState<string>("all");

  // Optional date filter – starts as null ("All dates"), only applied when set
  const [dateFilter, setDateFilter] = useState<string | null>(null);

  const [showScrollTop, setShowScrollTop] = useState(false);

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
        const [eventsRes, teamsRes] = await Promise.all([
          api.events(),
          api.teams(),
        ]);
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

  // 2) After events load, default Season filter to latest year ("current season")
  useEffect(() => {
    if (events.length === 0) return;
    if (yearFilter !== "all") return;

    const years = Array.from(
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
  }, [events, yearFilter]);

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

  // Team lookup using shared helpers
  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);

  function teamLabel(id: number | null): string {
    return teamLabelFromMap(teamsById, id);
  }

  // Sport filters (UI only)
  const sportFilters: { id: SportFilterId; label: string }[] = SPORT_FILTERS;

  // Year options derived from events – for the Season dropdown
  const yearOptions = useMemo(() => {
    const years = new Set<string>();
    for (const e of events) {
      const y = getYearFromDate(e.date);
      if (y) years.add(y);
    }
    return Array.from(years).sort();
  }, [events]);

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

  // ================================
  // Composed filtering logic (order)
  // 1) Season (yearFilter)
  // 2) Sport (selectedSport)
  // 3) Team (teamFilter)
  // 4) Optional Date (dateFilter)
  // ================================
  const visibleEvents = useMemo(() => {
    let filtered = events;

    // 1) Season filter – required / primary
    if (yearFilter !== "all") {
      filtered = filtered.filter((e) => getYearFromDate(e.date) === yearFilter);
    }

    // 2) Sport filter
    if (selectedSport !== "all") {
      filtered = filtered.filter((e) => e.sport_id === selectedSport);
    }

    // 3) Team filter
    if (teamFilter !== "all") {
      filtered = filtered.filter((e) => {
        const homeName = teamLabel(e.home_team_id);
        const awayName = teamLabel(e.away_team_id);
        return homeName === teamFilter || awayName === teamFilter;
      });
    }

    // 4) Optional Date filter – only applied if user selected a date
    if (dateFilter) {
      filtered = filtered.filter((e) => e.date === dateFilter);
    }

    // 5) Remove bogus NFL rows that would render as "TBD @ TBD"
    filtered = filtered.filter((e) => {
      if (e.sport_id !== 3) return true;

      // Prefer explicit team name fields from the event, then shared team lookup
      const rawHomeName =
        (e as any).home_team_name ??
        (e as any).home_team ??
        null;
      const rawAwayName =
        (e as any).away_team_name ??
        (e as any).away_team ??
        null;

      let homeTeamName: string | null =
        rawHomeName || teamLabel(e.home_team_id) || null;
      let awayTeamName: string | null =
        rawAwayName || teamLabel(e.away_team_id) || null;

      const expandNflName = (name: string | null): string | null => {
        if (!name) return name;
        const key = name.toUpperCase().trim();
        return NFL_TEAM_FULL_NAME_BY_ABBR[key] ?? name;
      };

      homeTeamName = expandNflName(homeTeamName);
      awayTeamName = expandNflName(awayTeamName);

      const derived = deriveNflNamesFromEventId(
        (e as any).event_id ?? e.event_id,
      );
      if (!homeTeamName && derived.home) {
        homeTeamName = derived.home;
      }
      if (!awayTeamName && derived.away) {
        awayTeamName = derived.away;
      }

      const isNflTbdMatchup =
        (!homeTeamName || homeTeamName === "TBD") &&
        (!awayTeamName || awayTeamName === "TBD");

      return !isNflTbdMatchup;
    });

    return filtered;
  }, [events, yearFilter, selectedSport, teamFilter, dateFilter]);

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
    <main className="min-h-screen bg-black px-4 pb-12 pt-7 text-white">
      <div className="mx-auto w-full max-w-6xl space-y-5">
        {/* ========================= */}
        {/* 1. PAGE HEADER            */}
        {/* ========================= */}
        <section className="rounded-2xl border border-slate-900 bg-gradient-to-b from-slate-950/90 to-black/90 px-5 py-4 shadow-sm shadow-black/40">
          <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">
                Games
              </h1>
              <p className="mt-1 max-w-xl text-xs text-zinc-400 sm:text-sm">
                Browse live lines and historical results across leagues. Fan
                view powered by your SportIQ models.
              </p>
            </div>
          </header>
        </section>

        {/* ========================= */}
        {/* 2. FILTER BAR             */}
        {/* ========================= */}
        <section className="space-y-3 rounded-2xl border border-zinc-900/80 bg-black/90 px-4 py-3 shadow-sm shadow-black/40">
          <div className="space-y-2.5">
            {/* Tier 1: Date + Today + Sport pills */}
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              {/* Date controls */}
              <div className="flex flex-wrap items-center gap-2 text-xs">
                <div className="flex items-center gap-2">
                  {/* Previous day */}
                  <button
                    type="button"
                    onClick={() => handleShiftDate(-1)}
                    className="flex h-8 w-8 items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/90 text-xs text-zinc-300 shadow-sm shadow-black/30 transition-all duration-150 hover:border-blue-500 hover:bg-blue-500/10 hover:text-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70 active:scale-95"
                    aria-label="Previous day"
                  >
                    ←
                  </button>

                  {/* Calendar style date input */}
                  <div className="relative flex items-center">
                    <span className="pointer-events-none absolute left-2 text-[13px] text-zinc-500">
                      📅
                    </span>
                    <input
                      type="date"
                      value={dateFilter ?? ""}
                      onChange={(e) =>
                        setDateFilter(
                          e.target.value === "" ? null : e.target.value,
                        )
                      }
                      className="min-w-[170px] rounded-full border border-zinc-700/80 bg-zinc-950 pl-7 pr-3 py-1.5 text-xs text-zinc-100 shadow-sm shadow-black/50 transition-all duration-150 focus:outline-none focus-visible:border-blue-500/80 focus-visible:ring-2 focus-visible:ring-blue-500/70"
                    />
                  </div>

                  {/* Next day */}
                  <button
                    type="button"
                    onClick={() => handleShiftDate(1)}
                    className="flex h-8 w-8 items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/90 text-xs text-zinc-300 shadow-sm shadow-black/30 transition-all duration-150 hover:border-blue-500 hover:bg-blue-500/10 hover:text-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70 active:scale-95"
                    aria-label="Next day"
                  >
                    →
                  </button>

                  {/* Today ghost-button */}
                  <button
                    type="button"
                    onClick={handleJumpToToday}
                    title="Jump to today"
                    className="rounded-full border border-zinc-700/80 bg-transparent px-2.5 py-1 text-[11px] text-zinc-200 shadow-sm shadow-black/20 transition-all duration-150 hover:border-blue-500 hover:bg-blue-500/10 hover:text-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70 active:scale-95"
                  >
                    Today
                  </button>
                </div>
              </div>

              {/* Sport pills */}
              <div className="flex w-full items-center gap-2 sm:w-auto">
                <div className="flex gap-2 overflow-x-auto py-1 text-[11px] scrollbar-thin scrollbar-track-transparent scrollbar-thumb-zinc-800">
                  {sportFilters.map((f) => (
                    <button
                      key={f.id}
                      type="button"
                      onClick={() => setSelectedSport(f.id)}
                      aria-pressed={selectedSport === f.id}
                      className={
                        "shrink-0 rounded-full border px-3 py-1.5 text-xs shadow-sm transition-all duration-150 " +
                        (selectedSport === f.id
                          ? "border-blue-400 bg-blue-500/25 text-blue-50 shadow-blue-500/40 ring-1 ring-blue-500/60"
                          : "border-zinc-700 bg-zinc-950/80 text-zinc-400 hover:border-zinc-500 hover:bg-zinc-900 hover:text-zinc-200 active:scale-95")
                      }
                    >
                      <span className="inline-flex items-center gap-1">
                        {f.id !== "all" && (
                          <span className="text-[12px]">
                            {sportIconFromId(f.id as number)}
                          </span>
                        )}
                        <span>{f.label}</span>
                      </span>
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {/* Tier 2: Season + Team + mini insight */}
            <div className="flex flex-col gap-3 border-t border-zinc-800/80 pt-2.5 text-xs sm:flex-row sm:items-center sm:justify-between">
              {/* Season + Team */}
              <div className="flex flex-wrap items-center gap-4">
                {/* Season selector */}
                <div className="flex items-center gap-2">
                  <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                    Season
                  </span>
                  <div className="relative inline-flex items-center">
                    <select
                      value={yearFilter}
                      onChange={(e) => {
                        const value = e.target.value;
                        setYearFilter(value);
                        // Clear any specific date filter when switching seasons
                        setDateFilter(null);
                      }}
                      className="rounded-full border border-zinc-700/80 bg-zinc-950 px-3 py-1.5 pr-7 text-xs text-zinc-100 shadow-sm shadow-black/40 transition focus:outline-none focus-visible:border-blue-500/80 focus-visible:ring-2 focus-visible:ring-blue-500/70"
                    >
                      <option value="all">All years</option>
                      {yearOptions.map((y) => (
                        <option key={y} value={y}>
                          {y}
                        </option>
                      ))}
                    </select>
                    <span className="pointer-events-none absolute right-2 text-[10px] text-zinc-500">
                      ⌄
                    </span>
                  </div>
                </div>

                {/* Team selector */}
                <div className="flex items-center gap-2">
                  <span className="flex items-center gap-1 text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                    Team
                    <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/90 px-1.5 py-[2px] text-[9px] font-medium text-zinc-300">
                      <span className="h-1.5 w-1.5 rounded-full bg-blue-400/80" />
                      {teamLeagueLabel}
                    </span>
                  </span>
                  <div className="relative inline-flex items-center">
                    <select
                      value={teamFilter}
                      onChange={(e) => setTeamFilter(e.target.value)}
                      disabled={isTeamDisabled}
                      className={
                        "rounded-full border px-3 py-1.5 pr-7 text-xs shadow-sm shadow-black/40 transition focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70 " +
                        (isTeamDisabled
                          ? "cursor-not-allowed border-zinc-800 bg-zinc-950/60 text-zinc-500"
                          : "border-zinc-700/80 bg-zinc-950 text-zinc-100 focus-visible:border-blue-500/80")
                      }
                    >
                      <option value="all">
                        {isTeamDisabled
                          ? "Select a sport to filter teams"
                          : "All teams"}
                      </option>
                      {!isTeamDisabled &&
                        teamOptions.map((name) => (
                          <option key={name} value={name}>
                            {name}
                          </option>
                        ))}
                    </select>
                    <span className="pointer-events-none absolute right-2 text-[10px] text-zinc-500">
                      ⌄
                    </span>
                  </div>
                </div>
              </div>

              {/* Mini insight (loading + errors handled below) */}
              <div className="mt-1 flex flex-1 items-center justify-end gap-2 sm:mt-0">
                {loading && (
                  <span className="text-[10px] text-zinc-500">
                    Loading games…
                  </span>
                )}
                {error && (
                  <span className="text-[10px] text-red-400">{error}</span>
                )}
              </div>
            </div>

            {/* Empty-state helper message */}
            {!loading && !error && visibleEvents.length === 0 && (
              <div className="mt-1 rounded-xl border border-zinc-800 bg-zinc-950/80 px-4 py-3 text-[11px] text-zinc-400">
                <p className="font-medium text-zinc-200">No games found</p>
                <p className="mt-1">
                  Try changing the date, selecting a different sport, or clearing
                  your team filter.
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
        <section className="pt-1">
          {isTruncated && (
            <div className="mb-3 rounded-xl border border-zinc-800 bg-zinc-950/80 px-4 py-2 text-[11px] text-zinc-400">
              Showing the first {MAX_EVENTS_TO_RENDER} games. Try narrowing the
              date, season, sport, or team filters to see a smaller set.
            </div>
          )}
          <div className="grid gap-5 sm:grid-cols-2">
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

              const isFinal = hasScores || e.status === "final";

              // Determine homeWin from field or from scores
              const homeWin =
                e.home_win != null
                  ? Boolean(e.home_win)
                  : hasScores
                  ? homeScore! > awayScore!
                  : null;

              const homeIsWinner = isFinal && homeWin === true;
              const awayIsWinner = isFinal && homeWin === false;

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

              const homeOddsToShow = hasSportsbookOdds
                ? bookHomeOddsRaw
                : modelHomeOddsRaw;
              const awayOddsToShow = hasSportsbookOdds
                ? bookAwayOddsRaw
                : modelAwayOddsRaw;

              const homeOddsFormatted = formatAmericanOdds(homeOddsToShow);
              const awayOddsFormatted = formatAmericanOdds(awayOddsToShow);

              const status = getEventStatus(e, isFinal);

              const statusClasses =
                status === "final"
                  ? "border-emerald-500/60 bg-emerald-500/10 text-emerald-300"
                  : status === "live"
                  ? "border-red-500/70 bg-red-500/10 text-red-300"
                  : "border-zinc-700/70 bg-zinc-900/70 text-zinc-200";

              const statusLabel =
                status === "final"
                  ? "Final"
                  : status === "live"
                  ? "Live"
                  : "Scheduled";

              const oddsLabel = isFinal
                ? null
                : hasSportsbookOdds
                ? "Sportsbook odds"
                : "Model odds";

              const modelHomeFormatted =
                formatAmericanOdds(modelHomeOddsRaw);
              const modelAwayFormatted =
                formatAmericanOdds(modelAwayOddsRaw);

              // Prefer explicit team name fields from the event (needed for NFL),
              // then fall back to the shared teams lookup, then finally "TBD".
              const rawHomeName =
                (e as any).home_team_name ??
                (e as any).home_team ??
                null;
              const rawAwayName =
                (e as any).away_team_name ??
                (e as any).away_team ??
                null;

              // Base names from explicit event fields or shared team lookup
              let homeTeamName: string | null =
                rawHomeName || teamLabel(e.home_team_id) || null;
              let awayTeamName: string | null =
                rawAwayName || teamLabel(e.away_team_id) || null;

              // For NFL, manually map common abbreviations (JAX, TEN, NO, TB, etc.)
              // to full team names so the UI always shows the full franchise name.
              if (e.sport_id === 3) {
                const expandNflName = (name: string | null): string | null => {
                  if (!name) return name;
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

              // Final fallback
              const finalHomeTeamName = homeTeamName ?? "TBD";
              const finalAwayTeamName = awayTeamName ?? "TBD";

              // 🚫 Hide bogus NFL rows where we never resolved either team.
              // These show up as "TBD @ TBD" and likely aren't real games.
              const isNflTbdMatchup =
                e.sport_id === 3 &&
                (!finalHomeTeamName || finalHomeTeamName === "TBD") &&
                (!finalAwayTeamName || finalAwayTeamName === "TBD");

              if (isNflTbdMatchup) {
                // Skip rendering this card entirely
                return null;
              }

              return (
                <Link
                  key={e.event_id}
                  href={`/games/${e.event_id}`}
                  prefetch={false}
                  className="group relative flex flex-col gap-3 overflow-hidden rounded-2xl border border-zinc-800/90 bg-gradient-to-br from-zinc-950/95 via-black to-zinc-950/90 px-5 py-4 shadow-sm shadow-black/60 transition-all duration-200 hover:-translate-y-[2px] hover:border-blue-500/70 hover:shadow-blue-500/25 active:scale-[0.99]"
                >
                  {/* Vertical accent bar */}
                  <div className="pointer-events-none absolute left-0 top-0 h-full w-1 bg-gradient-to-b from-blue-500 via-sky-500 to-transparent opacity-70" />

                  {/* Top row: matchup + league/status badges */}
                  <div className="flex items-start justify-between gap-3 pl-1">
                    <div className="min-w-0 flex-1">
                      {/* Matchup: main headline */}
                      <h2 className="truncate text-sm font-semibold leading-snug text-zinc-50 sm:text-[15px]">
                        {finalAwayTeamName}{" "}
                        <span className="text-zinc-500">@</span>{" "}
                        {finalHomeTeamName}
                      </h2>

                      {/* Date row */}
                      <p className="mt-1 text-[11px] text-zinc-400">
                        {formatReadableDate(e.date) ?? e.date}
                      </p>
                    </div>

                    {/* League + status badges */}
                    <div className="flex flex-col items-end gap-1">
                      <span className="inline-flex items-center gap-1 rounded-full border border-zinc-700 bg-zinc-900/80 px-2 py-0.5 text-[9px] uppercase tracking-[0.16em] text-zinc-300">
                        <span className="text-xs">
                          {sportIconFromId(e.sport_id)}
                        </span>
                        <span>{sportLabelFromId(e.sport_id)}</span>
                      </span>

                      <span
                        className={
                          "inline-flex items-center rounded-full border px-2 py-0.5 text-[9px] uppercase tracking-[0.16em] " +
                          statusClasses
                        }
                      >
                        {status === "live" && (
                          <span className="mr-1 inline-flex h-1.5 w-1.5 animate-pulse rounded-full bg-red-400" />
                        )}
                        {statusLabel}
                      </span>
                    </div>
                  </div>

                  {/* Value block – final vs scheduled */}
                  {isFinal && hasScores && homeScore !== null && awayScore !== null ? (
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
                    <OddsBlock
                      homeTeam={finalHomeTeamName}
                      awayTeam={finalAwayTeamName}
                      homeValue={homeOddsFormatted}
                      awayValue={awayOddsFormatted}
                      oddsLabel={oddsLabel}
                      modelHomeFormatted={modelHomeFormatted}
                      modelAwayFormatted={modelAwayFormatted}
                    />
                  )}

                  {/* Footer actions */}
                  <div className="mt-3 flex items-center justify-between border-t border-zinc-800/70 pt-2.5 pl-1 text-[11px]">
                    <span
                      className="inline-flex items-center gap-1 text-zinc-500"
                      title="Model powered by your NBA logreg engine"
                    >
                      <span className="h-1.5 w-1.5 rounded-full bg-blue-400/80" />
                      <span className="font-mono text-[10px] text-zinc-300/90">
                        model: logreg_v1
                      </span>
                    </span>
                    <div className="flex items-center gap-2 text-blue-400 group-hover:text-blue-300">
                      <span className="inline-flex items-center gap-1 rounded-full bg-blue-500/10 px-2 py-0.5 text-[10px]">
                        <span className="text-xs">📊</span>
                        <span>Box score</span>
                      </span>
                      <span className="inline-flex items-center gap-1 rounded-full bg-blue-500/10 px-2 py-0.5 text-[10px]">
                        <span className="text-xs">🤖</span>
                        <span>AI view</span>
                      </span>
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
          className="fixed bottom-6 right-6 inline-flex h-10 w-10 items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/90 text-zinc-200 shadow-lg shadow-black/50 backdrop-blur transition-all duration-150 hover:border-blue-500 hover:bg-blue-500/20 hover:text-blue-100 active:scale-95"
          aria-label="Back to top"
        >
          ↑
        </button>
      )}
    </main>
  );
}