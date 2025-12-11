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

  const [selectedSide, setSelectedSide] = useState<"home" | "away" | null>(
    null,
  );

  const [insights, setInsights] = useState<Insight[] | null>(null);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);

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
    <main className="min-h-screen bg-black px-4 pb-16 pt-8 text-white">
      <div className="mx-auto flex w-full max-w-5xl flex-col gap-8">
        {/* Back link */}
        <div className="flex items-center justify-between">
          <Link
            href="/games"
            className="inline-flex items-center gap-1 text-xs text-zinc-400 transition-colors hover:text-sky-300"
          >
            <span className="text-sm">←</span>
            <span>Back to games</span>
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
            {/* 1. HEADER / HERO          */}
            {/* ========================= */}
            <section className="relative overflow-hidden rounded-3xl border border-zinc-800 bg-gradient-to-br from-zinc-950 via-slate-950 to-black p-5 shadow-xl shadow-black/70">
              {/* Subtle stadium spotlight */}
              <div className="pointer-events-none absolute inset-x-6 -top-10 -z-10 h-32 rounded-full bg-[radial-gradient(circle_at_top,_rgba(56,189,248,0.22)_0,_transparent_60%)] blur-3xl" />

              <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
                {/* Left: matchup + meta */}
                <div className="space-y-3">
                  <div className="inline-flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-2xl border border-zinc-700 bg-zinc-900/90 text-xl">
                      {sportIcon}
                    </div>
                    <div className="flex flex-col">
                      <span className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                        {sportLabel}
                      </span>
                      <span className="text-[11px] text-zinc-400">
                        {formatDateOnly(event.date)}
                      </span>
                    </div>
                  </div>

                  <div className="space-y-1">
                    <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">
                      {awayName}{" "}
                      <span className="text-base text-zinc-500 sm:text-xl">@</span>{" "}
                      {homeName}
                    </h1>
                    <p className="text-xs text-zinc-400">
                      {formatDateTime(event.date)}
                    </p>
                  </div>

                  {/* Tags */}
                  <div className="flex flex-wrap items-center gap-2 text-[10px]">
                    <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/90 px-2.5 py-1 font-medium uppercase tracking-[0.16em] text-emerald-300">
                      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" />
                      Edge: {edgeCategory ?? "N/A"}
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/90 px-2.5 py-1 text-zinc-400">
                      Game ID:
                      <span className="font-mono text-zinc-100">
                        {event.event_id}
                      </span>
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/90 px-2.5 py-1 text-zinc-400">
                      🤖 Generated using your{" "}
                      <span className="font-medium text-zinc-100">
                        {sportLabel.toLowerCase()} surface
                      </span>
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/90 px-2.5 py-1 text-zinc-500">
                      {statusLabel}
                    </span>
                  </div>
                </div>

                {/* Right: score + confidence */}
                <div className="flex flex-col gap-3 rounded-2xl border border-zinc-800 bg-zinc-950/85 p-4 text-xs shadow-sm shadow-black/50">
                  {isFinal && (
                    <div className="flex items-center justify-between gap-4">
                      <div className="flex-1">
                        <p className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                          Final score
                        </p>
                        <p className="mt-1 text-sm font-medium text-zinc-100">
                          {awayName} {event.away_score}{" "}
                          <span className="mx-1 text-zinc-500">@</span>
                          {homeName} {event.home_score}
                        </p>
                      </div>
                      <div className="flex flex-col items-end text-[11px] text-zinc-400">
                        <span>Winner</span>
                        <span className="mt-1 rounded-full bg-zinc-900/90 px-2 py-1 text-[11px] text-zinc-100">
                          {event.home_win ? homeName : awayName}
                        </span>
                      </div>
                    </div>
                  )}

                  {favoriteWinProb != null && favoriteTeamLabel && (
                    <div className="space-y-2">
                      <div className="flex items-center justify-between text-[11px] text-zinc-400">
                        <span>Model confidence</span>
                        <span>
                          {favoriteTeamLabel} •{" "}
                          <span className="text-zinc-100">
                            {(favoriteWinProb * 100).toFixed(1)}%
                          </span>
                        </span>
                      </div>
                      <div className="relative h-2.5 overflow-hidden rounded-full bg-zinc-900">
                        <div
                          className={`absolute inset-y-0 left-0 ${edgeFillClass} bg-gradient-to-r from-current/40 via-current to-current/80 transition-all`}
                          style={{ width: `${edgeWidthPct}%` }}
                        />
                        <div className="pointer-events-none absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent" />
                      </div>
                      <div className="flex items-center justify-between text-[10px] text-zinc-500">
                        <span>
                          {awayName}:{" "}
                          {safePercent(prediction?.p_away)}
                        </span>
                        <span>
                          {homeName}:{" "}
                          {safePercent(prediction?.p_home)}
                        </span>
                      </div>
                    </div>
                  )}

                  <p className="mt-1 text-[10px] text-zinc-500">
                    Confidence reflects how far apart the model&apos;s win
                    probabilities are, not a guarantee of outcome.
                  </p>
                </div>
              </div>
            </section>

            {/* ========================= */}
            {/* 2. MODEL PREDICTION GRID  */}
            {/* ========================= */}
            <section className="grid gap-4 md:grid-cols-[minmax(0,3fr)_minmax(0,2.4fr)]">
              {/* Left: Model prediction */}
              <div className="rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-4 shadow-sm shadow-black/60">
                <div className="mb-3 flex items-center justify-between gap-2">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                      Model prediction
                    </p>
                    <p className="mt-0.5 text-xs text-zinc-400">
                      Side-by-side win probabilities. Bigger bar = stronger
                      belief.
                    </p>
                  </div>
                  <span className="rounded-full bg-zinc-900/90 px-2 py-1 text-[10px] text-zinc-400">
                    Pre-game surface
                  </span>
                </div>

                {predLoading && (
                  <p className="text-xs text-zinc-500">
                    Loading model prediction…
                  </p>
                )}

                {!predLoading && predError && (
                  <div className="rounded-xl border border-zinc-800 bg-zinc-950/90 px-3 py-2 text-xs text-zinc-300">
                    {predError}
                  </div>
                )}

                {!predLoading && !predError && !prediction && (
                  <p className="text-xs text-zinc-500">
                    No prediction yet. Check back once the model has scored
                    this game.
                  </p>
                )}

                {!predLoading && !predError && prediction && (
                  <>
                    <div className="mt-2 grid gap-3 md:grid-cols-2">
                      {/* Away side */}
                      <button
                        type="button"
                        onClick={() =>
                          setSelectedSide((prev) =>
                            prev === "away" ? null : "away",
                          )
                        }
                        className={
                          "group relative overflow-hidden rounded-2xl border px-3 py-3 text-left text-xs shadow-sm transition-all " +
                          (selectedSide === "away"
                            ? "border-sky-400 bg-sky-500/10 shadow-sky-500/30"
                            : "border-zinc-800 bg-zinc-950/80 hover:border-zinc-700")
                        }
                      >
                        {selectedSide === "away" && (
                          <div className="pointer-events-none absolute inset-0 bg-gradient-to-br from-sky-500/10 via-transparent to-emerald-500/5 opacity-80" />
                        )}
                        <div className="relative flex items-center justify-between gap-2">
                          <div>
                            <p className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                              Away side
                            </p>
                            <p className="mt-0.5 text-sm font-semibold text-zinc-100">
                              {prediction.away_team || awayName}
                            </p>
                          </div>
                          {selectedSide === "away" && (
                            <span className="rounded-full bg-sky-500/20 px-2 py-1 text-[10px] font-medium uppercase tracking-[0.16em] text-sky-100">
                              Selected
                            </span>
                          )}
                        </div>

                        <div className="relative mt-3 h-2.5 overflow-hidden rounded-full bg-zinc-900">
                          <div
                            className="absolute inset-y-0 left-0 bg-gradient-to-r from-sky-400/80 via-emerald-400/80 to-blue-500/80 transition-all"
                            style={{
                              width: `${Math.min(
                                100,
                                Math.max(
                                  0,
                                  (prediction.p_away || 0) * 100,
                                ),
                              )}%`,
                            }}
                          />
                        </div>

                        <div className="mt-2 flex items-end justify-between text-[11px] text-zinc-400">
                          <div className="space-y-0.5">
                            <p className="text-zinc-500">Win probability</p>
                            <p className="text-base font-semibold text-zinc-50">
                              {safePercent(prediction.p_away)}
                            </p>
                          </div>
                          <div className="space-y-0.5 text-right">
                            <p className="text-zinc-500">Implied odds</p>
                            <p className="font-mono text-[11px] text-zinc-200">
                              {impliedOdds(prediction.p_away)}
                            </p>
                          </div>
                        </div>
                      </button>

                      {/* Home side */}
                      <button
                        type="button"
                        onClick={() =>
                          setSelectedSide((prev) =>
                            prev === "home" ? null : "home",
                          )
                        }
                        className={
                          "group relative overflow-hidden rounded-2xl border px-3 py-3 text-left text-xs shadow-sm transition-all " +
                          (selectedSide === "home"
                            ? "border-sky-400 bg-sky-500/10 shadow-sky-500/30"
                            : "border-zinc-800 bg-zinc-950/80 hover:border-zinc-700")
                        }
                      >
                        {selectedSide === "home" && (
                          <div className="pointer-events-none absolute inset-0 bg-gradient-to-br from-sky-500/10 via-transparent to-emerald-500/5 opacity-80" />
                        )}
                        <div className="relative flex items-center justify-between gap-2">
                          <div>
                            <p className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                              Home side
                            </p>
                            <p className="mt-0.5 text-sm font-semibold text-zinc-100">
                              {prediction.home_team || homeName}
                            </p>
                          </div>
                          {selectedSide === "home" && (
                            <span className="rounded-full bg-sky-500/20 px-2 py-1 text-[10px] font-medium uppercase tracking-[0.16em] text-sky-100">
                              Selected
                            </span>
                          )}
                        </div>

                        <div className="relative mt-3 h-2.5 overflow-hidden rounded-full bg-zinc-900">
                          <div
                            className="absolute inset-y-0 left-0 bg-gradient-to-r from-sky-400/80 via-emerald-400/80 to-blue-500/80 transition-all"
                            style={{
                              width: `${Math.min(
                                100,
                                Math.max(
                                  0,
                                  (prediction.p_home || 0) * 100,
                                ),
                              )}%`,
                            }}
                          />
                        </div>

                        <div className="mt-2 flex items-end justify-between text-[11px] text-zinc-400">
                          <div className="space-y-0.5">
                            <p className="text-zinc-500">Win probability</p>
                            <p className="text-base font-semibold text-zinc-50">
                              {safePercent(prediction.p_home)}
                            </p>
                          </div>
                          <div className="space-y-0.5 text-right">
                            <p className="text-zinc-500">Implied odds</p>
                            <p className="font-mono text-[11px] text-zinc-200">
                              {impliedOdds(prediction.p_home)}
                            </p>
                          </div>
                        </div>
                      </button>
                    </div>

                    <p className="mt-3 text-[10px] text-zinc-500">
                      These are pre-game model estimates. They do not account
                      for last-minute injuries, rest decisions, or breaking
                      news.
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

              {/* Right: Quick factor summary / SHAP-style */}
              <div className="rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-4 shadow-sm shadow-black/60">
                <div className="mb-3 flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <span className="flex h-7 w-7 items-center justify-center rounded-full bg-zinc-900/90 text-sm">
                      🧩
                    </span>
                    <div>
                      <p className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                        Why the model leans this way
                      </p>
                      <p className="mt-0.5 text-xs text-zinc-400">
                        Top factor-level drivers behind the prediction.
                      </p>
                    </div>
                  </div>
                  <span className="rounded-full bg-zinc-900/90 px-2 py-1 text-[10px] text-zinc-400">
                    SHAP-style view
                  </span>
                </div>

                {insightsLoading && (
                  <p className="text-xs text-zinc-500">
                    Loading insights for this game…
                  </p>
                )}

                {!insightsLoading && insightsError && (
                  <div className="rounded-xl border border-zinc-800 bg-zinc-950/90 px-3 py-2 text-xs text-zinc-300">
                    {insightsError}
                  </div>
                )}

                {!insightsLoading &&
                  !insightsError &&
                  (!insights || insights.length === 0) && (
                    <p className="text-xs text-zinc-500">
                      No factor-level insights are available yet for this
                      matchup.
                    </p>
                  )}

                {!insightsLoading &&
                  !insightsError &&
                  insights &&
                  insights.length > 0 && (
                    <>
                      {/* Key factors */}
                      {keyFactors.length > 0 && (
                        <div className="space-y-2">
                          <p className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
                            Key factors
                          </p>
                          <div className="space-y-2">
                            {keyFactors.map((ins, idx) => {
                              const magnitude = Math.min(
                                Math.abs(ins.value ?? 0),
                                0.4,
                              );
                              const barWidth = 20 + magnitude * 200; // 20–100%

                              const positive = (ins.value ?? 0) >= 0;

                              return (
                                <div
                                  key={`${ins.type}-${ins.label}-${idx}`}
                                  className="group flex items-center justify-between gap-3 rounded-xl border border-zinc-800 bg-zinc-950/85 px-3 py-2.5 text-xs transition-colors hover:border-sky-500/70 hover:bg-zinc-900/85"
                                >
                                  <div className="min-w-0 flex-1 space-y-1">
                                    <div className="flex items-center gap-2">
                                      <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/90 px-2 py-0.5 text-[10px] font-medium text-zinc-100">
                                        {positive ? "Favors pick" : "Hurts pick"}
                                      </span>
                                      <span className="truncate text-[11px] text-zinc-400">
                                        {ins.label}
                                      </span>
                                    </div>
                                    {ins.detail && (
                                      <p className="line-clamp-2 text-[11px] text-zinc-400">
                                        {ins.detail}
                                      </p>
                                    )}
                                  </div>
                                  <div className="flex w-28 flex-col items-end gap-1">
                                    <div className="relative h-1.5 w-full overflow-hidden rounded-full bg-zinc-900">
                                      <div
                                        className={
                                          "absolute inset-y-0 left-0 transition-all " +
                                          (positive
                                            ? "bg-emerald-500/80"
                                            : "bg-red-500/80")
                                        }
                                        style={{ width: `${barWidth}%` }}
                                      />
                                    </div>
                                    {typeof ins.value === "number" && (
                                      <span
                                        className={
                                          "text-[10px] " +
                                          (positive
                                            ? "text-emerald-300"
                                            : "text-red-300")
                                        }
                                      >
                                        {(ins.value * 100).toFixed(1)}%
                                      </span>
                                    )}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      {/* Brief note */}
                      <p className="mt-3 text-[10px] text-zinc-500">
                        These aren&apos;t bets or advice – just a breakdown of
                        which inputs are nudging the model&apos;s win
                        probability up or down.
                      </p>
                    </>
                  )}
              </div>
            </section>

            {/* ========================= */}
            {/* 3. FULL EXPLANATION       */}
            {/* ========================= */}
            <section className="space-y-3 rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-4 shadow-sm shadow-black/60">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-zinc-900/90 text-sm">
                    📝
                  </span>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                      Full model narrative
                    </p>
                    <p className="mt-0.5 text-xs text-zinc-400">
                      A plain-language summary built from the factor-level
                      insights.
                    </p>
                  </div>
                </div>
                {explanationText && (
                  <button
                    type="button"
                    onClick={() => {
                      void navigator.clipboard.writeText(explanationText);
                    }}
                    className="inline-flex items-center gap-1 rounded-full border border-zinc-700 bg-zinc-950/80 px-3 py-1.5 text-[10px] font-medium text-zinc-200 transition hover:border-sky-500 hover:text-sky-100"
                  >
                    Copy explanation
                  </button>
                )}
              </div>

              {explanationText ? (
                <details className="group rounded-xl border border-zinc-800 bg-zinc-950/85 px-4 py-3 text-xs leading-relaxed text-zinc-200">
                  <summary className="flex cursor-pointer list-none items-center justify-between gap-2 text-[11px] text-zinc-400">
                    <span>Tap to expand full explanation</span>
                    <span className="text-xs transition-transform group-open:rotate-90">
                      ▸
                    </span>
                  </summary>
                  <div className="mt-3 space-y-2 text-[13px] leading-relaxed text-zinc-200">
                    {explanationText.split("\n\n").map((para, idx) => (
                      <p key={idx}>{para}</p>
                    ))}
                  </div>
                </details>
              ) : (
                <div className="rounded-xl border border-dashed border-zinc-700 bg-zinc-950/85 px-4 py-6 text-xs text-zinc-400">
                  A detailed narrative explanation hasn&apos;t been generated
                  yet for this game. As your surfaces mature, this section will
                  tell a fuller story about why the model leans the way it
                  does.
                </div>
              )}

              <p className="pt-1 text-[10px] text-zinc-500">
                SportIQ is an analytics prototype – not a sportsbook. Use these
                surfaces to understand games more deeply, not as direct betting
                advice.
              </p>
            </section>
          </>
        )}
      </div>
    </main>
  );
}