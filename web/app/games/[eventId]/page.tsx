"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import {
  api,
  type Event,
  type Team,
  type PredictResponse,
  type Insight,
  type OddsForEvent,
} from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap, getFullTeamName } from "@/lib/teams";

// --- Helpers --------------------------------------------------------

function isValidProb(p: number | null | undefined): p is number {
  return typeof p === "number" && !Number.isNaN(p);
}

function safePercent(p: number | null | undefined): string {
  if (!isValidProb(p)) return "–";
  return `${(p * 100).toFixed(1)}%`;
}

function impliedOdds(prob: number | null | undefined) {
  if (!isValidProb(prob) || prob <= 0) return "–";
  return `${(1 / prob).toFixed(2)}x`;
}

function parseApiError(
  err: unknown,
  notFoundMessage: string,
  defaultMessage: string,
): string {
  if (err instanceof Error) {
    if (err.message.includes("404")) return notFoundMessage;
    return err.message;
  }
  return defaultMessage;
}

function formatDateTime(dateStr: string | null | undefined) {
  if (!dateStr) return "Unknown date";
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return dateStr;
  return d.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatDateOnly(dateStr: string | null | undefined) {
  if (!dateStr) return "Today";
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return dateStr;
  return d.toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });
}

// -------------------------------------------------------------------

export default function GameDetailPage() {
  const { eventId: eventIdParam } = useParams() as { eventId: string };
  const eventId = Number(eventIdParam);

  const [event, setEvent] = useState<Event | null>(null);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [prediction, setPrediction] = useState<PredictResponse | null>(null);
  const [predLoading, setPredLoading] = useState(false);
  const [predError, setPredError] = useState<string | null>(null);

  const [insights, setInsights] = useState<Insight[] | null>(null);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);

  const [odds, setOdds] = useState<OddsForEvent | null>(null);
  const [oddsLoading, setOddsLoading] = useState(false);
  const [oddsError, setOddsError] = useState<string | null>(null);

  const [generatedAt] = useState(() => new Date().toISOString());

  const isFinal =
    event?.home_score != null &&
    event?.away_score != null &&
    event?.home_win != null;

  // ---------- Load event & teams ----------

  useEffect(() => {
    if (!eventIdParam || Number.isNaN(eventId)) {
      setLoading(false);
      setError("Invalid game id in URL.");
      return;
    }

    (async () => {
      try {
        setLoading(true);
        setError(null);

        const [eventsRes, teamsRes] = await Promise.all([
          api.events(),
          api.teams(),
        ]);

        const events = eventsRes.items || [];
        const found = events.find((e) => e.event_id === eventId) ?? null;

        setEvent(found);
        setTeams(teamsRes.items || []);

        if (!found) {
          setError("Game not found. Try going back to the board.");
        }
      } catch (err: unknown) {
        console.error(err);
        setError("Failed to load game details.");
      } finally {
        setLoading(false);
      }
    })();
  }, [eventId, eventIdParam]);

  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);
  const teamLabel = (id: number | null) => teamLabelFromMap(teamsById, id);

  // Prioritize event.home_team/away_team from API, fall back to ID lookup
  // For NHL, expand abbreviations to full names (e.g., "CGY" → "Calgary Flames")
  const getTeamDisplayName = (teamCode: string | null, sportId: number) => {
    if (!teamCode) return null;
    // For NHL, convert abbreviation to full name
    if (sportId === 4) {
      return getFullTeamName(sportId, teamCode);
    }
    return teamCode;
  };

  const homeName = event
    ? (getTeamDisplayName(event.home_team, event.sport_id) || teamLabel(event.home_team_id))
    : "Home";
  const awayName = event
    ? (getTeamDisplayName(event.away_team, event.sport_id) || teamLabel(event.away_team_id))
    : "Away";

  // ---------- Load prediction ----------

  useEffect(() => {
    if (!event) return;

    (async () => {
      try {
        setPredLoading(true);
        setPredError(null);
        setPrediction(null);
        setSelectedSide(null);

        const result = await api.predict(event.event_id);
        setPrediction(result);
      } catch (err: unknown) {
        if (process.env.NODE_ENV === "development") {
          console.warn("Prediction API error", err);
        }
        const message = parseApiError(
          err,
          "No model prediction is available yet for this game.",
          "Failed to load prediction.",
        );
        setPredError(message);
      } finally {
        setPredLoading(false);
      }
    })();
  }, [event]);

  // ---------- Load insights ----------

  useEffect(() => {
    if (!event) return;

    (async () => {
      try {
        setInsightsLoading(true);
        setInsightsError(null);
        setInsights(null);

        const data = await api.insights(event.event_id);
        setInsights(data.insights || []);
      } catch (err: unknown) {
        if (process.env.NODE_ENV === "development") {
          console.warn("Insights API error", err);
        }
        const message = parseApiError(
          err,
          "No insights are available yet for this game.",
          "Failed to load insights.",
        );
        setInsightsError(message);
      } finally {
        setInsightsLoading(false);
      }
    })();
  }, [event]);

  // ---------- Load odds ----------

  useEffect(() => {
    if (!event || !homeName || !awayName) return;

    // Only load for upcoming games (not final)
    if (isFinal) return;

    (async () => {
      try {
        setOddsLoading(true);
        setOddsError(null);
        setOdds(null);

        // Map sport_id to sport code
        const sportCode =
          event.sport_id === 1 ? "nba" :
          event.sport_id === 2 ? "nfl" :
          event.sport_id === 4 ? "nhl" :
          undefined;

        const data = await api.oddsForEvent(homeName, awayName, sportCode);
        setOdds(data);
      } catch (err: unknown) {
        if (process.env.NODE_ENV === "development") {
          console.warn("Odds API error", err);
        }
        const message = parseApiError(
          err,
          "No odds are available yet for this game.",
          "Failed to load odds.",
        );
        setOddsError(message);
      } finally {
        setOddsLoading(false);
      }
    })();
  }, [event, homeName, awayName, isFinal]);

  // ---------- Derived model / edge info ----------

  const edgeCategory = useMemo(() => {
    if (!prediction) return null;
    if (!isValidProb(prediction.p_home) || !isValidProb(prediction.p_away)) {
      return null;
    }
    const edge = Math.abs(prediction.p_home - prediction.p_away);
    if (edge < 0.05) return "COIN FLIP";
    if (edge < 0.15) return "MODEST EDGE";
    return "STRONG FAVORITE";
  }, [prediction]);

  const edgeFillClass = useMemo(() => {
    if (edgeCategory === "COIN FLIP") return "bg-zinc-400";
    if (edgeCategory === "MODEST EDGE") return "bg-amber-400";
    if (edgeCategory === "STRONG FAVORITE") return "bg-emerald-500";
    return "bg-zinc-500";
  }, [edgeCategory]);

  const edge = useMemo(() => {
    if (!prediction) return null;
    if (!isValidProb(prediction.p_home) || !isValidProb(prediction.p_away)) {
      return null;
    }
    return Math.abs(prediction.p_home - prediction.p_away);
  }, [prediction]);

  const edgeWidthPct =
    edge !== null ? Math.min(Math.max(edge * 200, 5), 100) : 0;

  const favoriteSide: "home" | "away" | null = useMemo(() => {
    if (!prediction) return null;
    if (!isValidProb(prediction.p_home) || !isValidProb(prediction.p_away)) {
      return null;
    }
    return prediction.p_home >= prediction.p_away ? "home" : "away";
  }, [prediction]);

  const favoriteWinProb = useMemo(() => {
    if (!prediction || !favoriteSide) return null;
    const p =
      favoriteSide === "home" ? prediction.p_home : prediction.p_away;
    return isValidProb(p) ? p : null;
  }, [prediction, favoriteSide]);

  const favoriteTeamLabel =
    favoriteSide === "home" ? homeName : favoriteSide === "away" ? awayName : null;

  const keyFactors = useMemo(() => {
    if (!insights || insights.length === 0) return [];

    const preferredTypes = new Set([
      "favorite",
      "edge",
      "season_strength",
      "recent_form",
      "rest",
      "momentum",
    ]);

    const candidates = insights.filter((ins) => {
      const hasValue = ins.value != null && !Number.isNaN(ins.value);
      return preferredTypes.has(ins.type) && hasValue;
    });

    const sorted = candidates.sort((a, b) => {
      const va = a.value ?? 0;
      const vb = b.value ?? 0;
      return vb - va;
    });

    return sorted.slice(0, 3);
  }, [insights]);

  const predictionOutcome = useMemo(() => {
    if (!event || !isFinal || !prediction) return null;
    if (!isValidProb(prediction.p_home) || !isValidProb(prediction.p_away)) {
      return null;
    }

    const modelHomeWin = prediction.p_home >= prediction.p_away;
    const correct = modelHomeWin === !!event.home_win;

    return {
      correct,
      favoredSide: modelHomeWin ? "home" : "away",
    } as const;
  }, [event, isFinal, prediction]);

  const explanationText: string | null = useMemo(() => {
    if (!insights || insights.length === 0) return null;
    const lines = insights.map((ins) => {
      const valueText =
        typeof ins.value === "number"
          ? ` (${(ins.value * 100).toFixed(1)}%)`
          : "";
      return `${ins.label}${valueText}: ${ins.detail}`;
    });
    return lines.join("\n\n");
  }, [insights]);

  const sportLabel = event ? sportLabelFromId(event.sport_id) : "Sport";
  const sportIcon = event ? sportIconFromId(event.sport_id) : "🏟️";

  const statusLabel: string = isFinal ? "Final" : "Scheduled";

  // ------------------------------------------------------------------
  // Render
  // ------------------------------------------------------------------

  return (
    <main className="min-h-screen bg-black px-4 pb-16 pt-6 text-white">
      <div className="mx-auto flex w-full max-w-4xl flex-col gap-6">
        {/* Back link */}
        <div className="flex items-center justify-between px-1">
          <Link
            href="/games"
            className="inline-flex items-center gap-2 text-sm text-zinc-400 transition-colors hover:text-blue-400"
          >
            <span className="text-base">←</span>
            <span>All games</span>
          </Link>
        </div>

        {/* Loading / error states */}
        {loading && (
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950/90 px-4 py-6 text-xs text-zinc-400">
            <div className="flex items-center gap-2">
              <span className="h-2 w-2 animate-ping rounded-full bg-sky-400" />
              <span>Loading game details…</span>
            </div>
          </div>
        )}

        {!loading && error && (
          <div className="rounded-2xl border border-red-500/60 bg-red-500/10 px-4 py-6 text-xs text-red-100">
            <p className="font-medium">Unable to load this game.</p>
            <p className="mt-1 text-[11px] text-red-100/80">{error}</p>
          </div>
        )}

        {!loading && !error && !event && (
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950/80 px-4 py-6 text-xs text-zinc-300">
            This game could not be found in the schedule.
          </div>
        )}

        {/* Main content */}
        {!loading && !error && event && (
          <>
            {/* ========================= */}
            {/* 1. CLEAN HEADER           */}
            {/* ========================= */}
            <section className="space-y-4 px-1">
              {/* Meta row */}
              <div className="flex items-center gap-3">
                <span className="text-2xl">{sportIcon}</span>
                <div className="flex flex-col">
                  <span className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                    {sportLabel}
                  </span>
                  <span className="text-xs text-zinc-400">
                    {formatDateOnly(event.date)}
                  </span>
                </div>
                {isFinal && (
                  <span className="ml-auto rounded-md bg-emerald-500/15 px-3 py-1 text-xs font-semibold uppercase tracking-wider text-emerald-400">
                    Final
                  </span>
                )}
              </div>

              {/* Matchup */}
              <div>
                <h1 className="text-3xl font-bold tracking-tight sm:text-4xl">
                  {awayName}{" "}
                  <span className="text-xl text-zinc-600 sm:text-2xl">at</span>{" "}
                  {homeName}
                </h1>
                <p className="mt-1.5 text-sm text-zinc-500">
                  {formatDateTime(event.date)}
                </p>
              </div>

              {/* Final score if available */}
              {isFinal && (
                <div className="flex items-center gap-6 rounded-lg border border-zinc-800/50 bg-zinc-900/30 px-4 py-3">
                  <div className="flex flex-col">
                    <span className="text-xs text-zinc-500">Final Score</span>
                    <span className="mt-1 text-2xl font-bold tabular-nums text-zinc-200">
                      {event.away_score} — {event.home_score}
                    </span>
                  </div>
                  <div className="ml-auto flex items-center gap-2">
                    <span className="text-xs text-zinc-500">Winner:</span>
                    <span className="text-sm font-semibold text-emerald-400">
                      {event.home_win ? homeName : awayName}
                    </span>
                  </div>
                </div>
              )}
            </section>

            {/* ========================= */}
            {/* 2. THE TAKEAWAY (NEW)     */}
            {/* ========================= */}
            {!predLoading && !predError && prediction && (
              <section className="overflow-hidden rounded-xl border border-blue-900/30 bg-gradient-to-br from-blue-950/20 via-zinc-950/40 to-zinc-950/20 p-6">
                <div className="flex items-start gap-4">
                  <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-blue-500/10 text-2xl">
                    {edgeCategory === "COIN FLIP" ? "⚖️" : edgeCategory === "MODEST EDGE" ? "📊" : "⭐"}
                  </div>
                  <div className="flex-1 space-y-2">
                    <h2 className="text-sm font-medium uppercase tracking-wider text-blue-400">
                      Model&apos;s Take
                    </h2>
                    <p className="text-lg font-semibold leading-snug text-white sm:text-xl">
                      {edgeCategory === "COIN FLIP" ? (
                        <>
                          This is essentially a <span className="text-zinc-400">coin flip</span>.
                          The model sees both teams within{" "}
                          <span className="text-blue-400">5 percentage points</span>.
                        </>
                      ) : edgeCategory === "MODEST EDGE" ? (
                        <>
                          The model leans slightly toward{" "}
                          <span className="text-blue-400">{favoriteTeamLabel}</span>
                          {favoriteWinProb && ` (${(favoriteWinProb * 100).toFixed(1)}%)`}, but
                          this is a <span className="text-zinc-400">modest edge</span>.
                        </>
                      ) : (
                        <>
                          The model strongly favors{" "}
                          <span className="text-blue-400">{favoriteTeamLabel}</span>
                          {favoriteWinProb && ` (${(favoriteWinProb * 100).toFixed(1)}%)`} in
                          this matchup.
                        </>
                      )}
                    </p>
                    <p className="text-xs text-zinc-500">
                      {edgeCategory === "COIN FLIP"
                        ? "Small probability differences like this are statistically insignificant. Either team could reasonably win."
                        : edgeCategory === "MODEST EDGE"
                        ? "The model sees some advantages, but there's meaningful uncertainty in this game."
                        : "This represents a clear mismatch based on the available data, though upsets always happen."}
                    </p>
                  </div>
                </div>
              </section>
            )}

            {/* ========================= */}
            {/* 3. MODEL PREDICTION GRID  */}
            {/* ========================= */}
            <section className="space-y-5">
              {/* Model prediction card */}
              <div className="overflow-hidden rounded-xl border border-zinc-800/60 bg-gradient-to-br from-zinc-900/40 to-zinc-950/60 p-5 shadow-xl shadow-black/40">
                <div className="mb-4 flex items-center justify-between">
                  <h2 className="text-sm font-medium uppercase tracking-wider text-zinc-400">
                    Win Probabilities
                  </h2>
                  <span className="rounded-md bg-zinc-900/60 px-2 py-1 text-[10px] text-zinc-500">
                    Pre-game estimate
                  </span>
                </div>

                {predLoading && (
                  <div className="flex items-center gap-2 text-sm text-zinc-500">
                    <span className="h-2 w-2 animate-pulse rounded-full bg-blue-400" />
                    <span>Loading prediction…</span>
                  </div>
                )}

                {!predLoading && predError && (
                  <div className="rounded-lg border border-zinc-800/50 bg-zinc-900/30 px-4 py-3 text-sm text-zinc-400">
                    {predError}
                  </div>
                )}

                {!predLoading && !predError && !prediction && (
                  <p className="text-sm text-zinc-500">
                    No prediction available yet for this game.
                  </p>
                )}

                {!predLoading && !predError && prediction && (
                  <>
                    {/* Simplified probability display */}
                    <div className="space-y-3">
                      {/* Away */}
                      <div className="rounded-lg border border-zinc-800/50 bg-zinc-900/30 p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <span className="text-xs text-zinc-500">Away</span>
                            <p className="mt-1 text-lg font-bold text-zinc-200">
                              {prediction.away_team || awayName}
                            </p>
                          </div>
                          <div className="text-right">
                            <p className="text-3xl font-bold tabular-nums text-white">
                              {safePercent(prediction.p_away)}
                            </p>
                            {prediction.p_away && prediction.p_away > 0 && (
                              <p className="mt-0.5 font-mono text-xs text-zinc-500">
                                {impliedOdds(prediction.p_away)}
                              </p>
                            )}
                          </div>
                        </div>
                        <div className="mt-3 h-2 overflow-hidden rounded-full bg-zinc-900">
                          <div
                            className="h-full bg-gradient-to-r from-blue-500/80 to-blue-600/60 transition-all duration-500"
                            style={{
                              width: `${Math.min(100, Math.max(0, (prediction.p_away || 0) * 100))}%`,
                            }}
                          />
                        </div>
                      </div>

                      {/* Home */}
                      <div className="rounded-lg border border-zinc-800/50 bg-zinc-900/30 p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <span className="text-xs text-zinc-500">Home</span>
                            <p className="mt-1 text-lg font-bold text-zinc-200">
                              {prediction.home_team || homeName}
                            </p>
                          </div>
                          <div className="text-right">
                            <p className="text-3xl font-bold tabular-nums text-white">
                              {safePercent(prediction.p_home)}
                            </p>
                            {prediction.p_home && prediction.p_home > 0 && (
                              <p className="mt-0.5 font-mono text-xs text-zinc-500">
                                {impliedOdds(prediction.p_home)}
                              </p>
                            )}
                          </div>
                        </div>
                        <div className="mt-3 h-2 overflow-hidden rounded-full bg-zinc-900">
                          <div
                            className="h-full bg-gradient-to-r from-blue-500/80 to-blue-600/60 transition-all duration-500"
                            style={{
                              width: `${Math.min(100, Math.max(0, (prediction.p_home || 0) * 100))}%`,
                            }}
                          />
                        </div>
                      </div>
                    </div>

                    <p className="mt-4 text-xs text-zinc-500">
                      Pre-game estimates based on available data. Does not account for last-minute roster changes or breaking news.
                    </p>

                    {isFinal && predictionOutcome && (
                      <div className="mt-4 rounded-2xl border border-zinc-800 bg-zinc-900/85 px-4 py-3 text-[11px] text-zinc-300">
                        <div className="mb-1 flex items-center justify-between">
                          <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                            Outcome vs. model
                          </span>
                          <span
                            className={
                              "inline-flex items-center rounded-full px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] " +
                              (predictionOutcome.correct
                                ? "border border-emerald-500/60 bg-emerald-500/10 text-emerald-300"
                                : "border border-red-500/60 bg-red-500/10 text-red-300")
                            }
                          >
                            {predictionOutcome.correct
                              ? "Model was correct"
                              : "Model was wrong"}
                          </span>
                        </div>
                        <p className="text-[11px] text-zinc-400">
                          Pre-game, the model leaned toward{" "}
                          <span className="font-medium text-zinc-100">
                            {predictionOutcome.favoredSide === "home"
                              ? homeName
                              : awayName}
                          </span>
                          .
                        </p>
                      </div>
                    )}

                    <p className="mt-3 text-[10px] text-zinc-500">
                      Generated at{" "}
                      <span className="text-zinc-300">
                        {formatDateTime(generatedAt)}
                      </span>
                      .
                    </p>
                  </>
                )}
              </div>

              {/* Sportsbook Odds (only for upcoming games) */}
              {!isFinal && (
                <div className="overflow-hidden rounded-xl border border-zinc-800/60 bg-gradient-to-br from-zinc-900/40 to-zinc-950/60 p-5 shadow-xl shadow-black/40">
                  <div className="mb-4 flex items-center justify-between">
                    <div>
                      <h2 className="flex items-center gap-2 text-sm font-medium uppercase tracking-wider text-zinc-400">
                        <span className="text-lg">💰</span>
                        <span>Sportsbook Odds</span>
                      </h2>
                      <p className="mt-1 text-xs text-zinc-500">
                        Live betting lines from major sportsbooks
                      </p>
                    </div>
                  </div>

                  {oddsLoading && (
                    <div className="flex items-center gap-2 text-sm text-zinc-500">
                      <span className="h-2 w-2 animate-pulse rounded-full bg-blue-400" />
                      <span>Loading odds…</span>
                    </div>
                  )}

                  {!oddsLoading && oddsError && (
                    <div className="rounded-lg border border-zinc-800/50 bg-zinc-900/30 px-4 py-3 text-sm text-zinc-400">
                      {oddsError}
                    </div>
                  )}

                  {!oddsLoading && !oddsError && !odds && (
                    <p className="text-sm text-zinc-500">
                      No odds data available for this matchup yet.
                    </p>
                  )}

                  {!oddsLoading && !oddsError && odds && (
                    <div className="space-y-4">
                      {/* Moneyline Odds */}
                      {odds.moneyline && odds.moneyline.length > 0 && (
                        <div className="space-y-3">
                          <h3 className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                            Moneyline
                          </h3>
                          <div className="grid gap-3 sm:grid-cols-2">
                            {/* Group by team */}
                            {[
                              { team: awayName, label: "Away" },
                              { team: homeName, label: "Home" },
                            ].map(({ team, label }) => {
                              const teamOdds = odds.moneyline.filter(
                                (o) => o.outcome_name === team
                              );
                              if (teamOdds.length === 0) return null;

                              return (
                                <div key={team} className="rounded-lg border border-zinc-800/50 bg-zinc-900/30 p-4">
                                  <div className="mb-3 flex items-center justify-between">
                                    <div>
                                      <span className="text-xs text-zinc-500">{label}</span>
                                      <p className="mt-0.5 font-semibold text-zinc-200">{team}</p>
                                    </div>
                                  </div>
                                  <div className="space-y-2">
                                    {teamOdds.map((odd, idx) => (
                                      <div
                                        key={`${odd.bookmaker}-${idx}`}
                                        className="flex items-center justify-between text-sm"
                                      >
                                        <span className="capitalize text-zinc-400">
                                          {odd.bookmaker.replace("_", " ")}
                                        </span>
                                        <span className="font-mono font-medium text-zinc-200">
                                          {odd.outcome_price > 0 ? "+" : ""}
                                          {odd.outcome_price}
                                        </span>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      {/* Spread Odds */}
                      {odds.spreads && odds.spreads.length > 0 && (
                        <div className="space-y-3">
                          <h3 className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                            Spreads
                          </h3>
                          <div className="grid gap-3 sm:grid-cols-2">
                            {[
                              { team: awayName, label: "Away" },
                              { team: homeName, label: "Home" },
                            ].map(({ team, label }) => {
                              const teamSpreads = odds.spreads.filter(
                                (o) => o.outcome_name === team
                              );
                              if (teamSpreads.length === 0) return null;

                              return (
                                <div key={team} className="rounded-lg border border-zinc-800/50 bg-zinc-900/30 p-4">
                                  <div className="mb-3 flex items-center justify-between">
                                    <div>
                                      <span className="text-xs text-zinc-500">{label}</span>
                                      <p className="mt-0.5 font-semibold text-zinc-200">{team}</p>
                                    </div>
                                  </div>
                                  <div className="space-y-2">
                                    {teamSpreads.map((spread, idx) => (
                                      <div
                                        key={`${spread.bookmaker}-${idx}`}
                                        className="flex items-center justify-between text-sm"
                                      >
                                        <span className="capitalize text-zinc-400">
                                          {spread.bookmaker.replace("_", " ")}
                                        </span>
                                        <span className="flex items-center gap-2">
                                          <span className="font-medium text-blue-400">
                                            {spread.point && spread.point > 0 ? "+" : ""}
                                            {spread.point}
                                          </span>
                                          <span className="font-mono text-xs text-zinc-500">
                                            ({spread.outcome_price > 0 ? "+" : ""}
                                            {spread.outcome_price})
                                          </span>
                                        </span>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      <p className="text-xs text-zinc-600">
                        Odds last updated:{" "}
                        {new Date(odds.moneyline[0]?.last_update_utc || odds.spreads[0]?.last_update_utc).toLocaleString()}
                      </p>
                    </div>
                  )}
                </div>
              )}

              {/* Why This Prediction Makes Sense */}
              <div className="overflow-hidden rounded-xl border border-zinc-800/60 bg-gradient-to-br from-zinc-900/40 to-zinc-950/60 p-5 shadow-xl shadow-black/40">
                <div className="mb-4">
                  <h2 className="flex items-center gap-2 text-sm font-medium uppercase tracking-wider text-zinc-400">
                    <span className="text-lg">📊</span>
                    <span>Why This Prediction</span>
                  </h2>
                  <p className="mt-1 text-xs text-zinc-500">
                    Key factors influencing the model&apos;s assessment
                  </p>
                </div>

                {insightsLoading && (
                  <div className="flex items-center gap-2 text-sm text-zinc-500">
                    <span className="h-2 w-2 animate-pulse rounded-full bg-blue-400" />
                    <span>Loading factors…</span>
                  </div>
                )}

                {!insightsLoading && insightsError && (
                  <div className="rounded-lg border border-zinc-800/50 bg-zinc-900/30 px-4 py-3 text-sm text-zinc-400">
                    {insightsError}
                  </div>
                )}

                {!insightsLoading &&
                  !insightsError &&
                  (!insights || insights.length === 0) && (
                    <p className="text-sm text-zinc-500">
                      No detailed factors available yet for this matchup.
                    </p>
                  )}

                {!insightsLoading &&
                  !insightsError &&
                  insights &&
                  insights.length > 0 && (
                    <>
                      {/* Key factors */}
                      {keyFactors.length > 0 && (
                        <div className="space-y-3">
                          {keyFactors.map((ins, idx) => {
                            const magnitude = Math.min(Math.abs(ins.value ?? 0), 0.4);
                            const barWidth = 20 + magnitude * 200; // 20–100%
                            const positive = (ins.value ?? 0) >= 0;
                            const favoredTeam = favoriteSide === "home" ? homeName : awayName;

                            return (
                              <div
                                key={`${ins.type}-${ins.label}-${idx}`}
                                className="group rounded-lg border border-zinc-800/50 bg-zinc-900/30 p-4 transition-all duration-200 hover:border-zinc-700/70 hover:bg-zinc-900/40"
                              >
                                <div className="flex items-start gap-3">
                                  <div
                                    className={
                                      "flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-sm " +
                                      (positive
                                        ? "bg-blue-500/10 text-blue-400"
                                        : "bg-zinc-800/50 text-zinc-500")
                                    }
                                  >
                                    {positive ? "+" : "−"}
                                  </div>
                                  <div className="flex-1 space-y-2">
                                    <div className="space-y-1">
                                      <p className="text-sm font-medium text-zinc-200">
                                        {ins.label}
                                      </p>
                                      {ins.detail && (
                                        <p className="text-xs leading-relaxed text-zinc-500">
                                          {ins.detail}
                                        </p>
                                      )}
                                    </div>
                                    <div className="flex items-center gap-2">
                                      <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-zinc-900">
                                        <div
                                          className={
                                            "h-full transition-all duration-500 " +
                                            (positive ? "bg-blue-500/60" : "bg-zinc-600/60")
                                          }
                                          style={{ width: `${barWidth}%` }}
                                        />
                                      </div>
                                      {typeof ins.value === "number" && (
                                        <span
                                          className={
                                            "text-xs font-medium tabular-nums " +
                                            (positive ? "text-blue-400" : "text-zinc-500")
                                          }
                                        >
                                          {positive ? "+" : ""}
                                          {(ins.value * 100).toFixed(1)}%
                                        </span>
                                      )}
                                    </div>
                                    <p className="text-[10px] text-zinc-600">
                                      {positive
                                        ? `Pushes toward ${favoredTeam}`
                                        : `Minimal impact on prediction`}
                                    </p>
                                  </div>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      )}

                      {/* Brief note */}
                      <p className="mt-4 text-xs text-zinc-500">
                        These factors show what data points influence the model, not betting recommendations.
                      </p>
                    </>
                  )}
              </div>
            </section>

            {/* ========================= */}
            {/* 4. CONTEXT & TRUST        */}
            {/* ========================= */}
            <section className="overflow-hidden rounded-xl border border-zinc-800/60 bg-gradient-to-br from-zinc-900/40 to-zinc-950/60 p-5 shadow-xl shadow-black/40">
              <div className="mb-4 flex items-start justify-between gap-3">
                <div>
                  <h2 className="flex items-center gap-2 text-sm font-medium uppercase tracking-wider text-zinc-400">
                    <span className="text-lg">💬</span>
                    <span>Plain-Language Summary</span>
                  </h2>
                  <p className="mt-1 text-xs text-zinc-500">
                    Model reasoning explained in everyday terms
                  </p>
                </div>
                {explanationText && (
                  <button
                    type="button"
                    onClick={() => {
                      void navigator.clipboard.writeText(explanationText);
                    }}
                    className="inline-flex items-center gap-1.5 rounded-lg border border-zinc-800 bg-zinc-900/60 px-3 py-2 text-xs font-medium text-zinc-400 transition-all duration-200 hover:border-blue-500/50 hover:bg-blue-500/10 hover:text-blue-400"
                  >
                    <span>📋</span>
                    <span>Copy</span>
                  </button>
                )}
              </div>

              {explanationText ? (
                <div className="space-y-3 rounded-lg border border-zinc-800/50 bg-zinc-900/30 p-4">
                  {explanationText.split("\n\n").map((para, idx) => (
                    <p key={idx} className="text-sm leading-relaxed text-zinc-300">
                      {para}
                    </p>
                  ))}
                </div>
              ) : (
                <div className="rounded-lg border border-dashed border-zinc-800/50 bg-zinc-900/20 px-4 py-8 text-center">
                  <p className="text-sm text-zinc-500">
                    Detailed narrative explanation not yet available
                  </p>
                  <p className="mt-2 text-xs text-zinc-600">
                    As the system matures, this section will provide deeper context about the model&apos;s reasoning
                  </p>
                </div>
              )}
            </section>

            {/* ========================= */}
            {/* 5. RESPONSIBLE USE        */}
            {/* ========================= */}
            <section className="rounded-lg border border-amber-900/30 bg-gradient-to-r from-amber-950/10 to-amber-900/5 px-5 py-4">
              <div className="flex items-start gap-3">
                <span className="text-xl">⚠️</span>
                <div className="flex-1 space-y-2">
                  <p className="text-sm font-medium text-amber-400/90">
                    This is a model interpretation tool, not betting advice
                  </p>
                  <p className="text-xs leading-relaxed text-amber-600/70">
                    SportIQ helps you understand what factors influence predictions. Models are statistical estimates based on historical data—they don&apos;t account for real-time events, injuries, or countless other variables. Use this to deepen your understanding of the game, not as direct gambling recommendations.
                  </p>
                </div>
              </div>
            </section>
          </>
        )}
      </div>
    </main>
  );
}