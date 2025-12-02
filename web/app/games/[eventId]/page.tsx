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
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";

// --- Probability helpers ---
function isValidProb(p: number | null | undefined): p is number {
  return typeof p === "number" && !Number.isNaN(p);
}

function safePercent(p: number | null | undefined): string {
  if (!isValidProb(p)) return "–";
  return `${(p * 100).toFixed(1)}%`;
}

function parseApiError(
  err: unknown,
  notFoundMessage: string,
  defaultMessage: string,
): string {
  if (err instanceof Error) {
    // Our api wrapper throws messages like "API /predict/nba failed: 404"
    if (err.message.includes("404")) {
      return notFoundMessage;
    }
    return err.message;
  }
  return defaultMessage;
}

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

  // ---------- Derived helpers ----------

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

  // Color for the confidence bar based on edge strength
  const edgeFillClass = useMemo(() => {
    if (edgeCategory === "COIN FLIP") {
      // neutral, low confidence
      return "bg-zinc-400";
    }
    if (edgeCategory === "MODEST EDGE") {
      // amber, medium confidence
      return "bg-amber-400";
    }
    if (edgeCategory === "STRONG FAVORITE") {
      // strong green, high confidence
      return "bg-emerald-500";
    }
    // fallback
    return "bg-zinc-500";
  }, [edgeCategory]);

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

  function impliedOdds(prob: number | null | undefined) {
    if (!prob || prob <= 0) return "-";
    return `${(1 / prob).toFixed(2)}x`;
  }

  // ---------- Load event & teams ----------

  useEffect(() => {
    if (!eventIdParam || Number.isNaN(eventId)) return;

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
          setError("Game not found");
        }
      } catch (err: unknown) {
        console.error(err);
        setError("Failed to load game");
      } finally {
        setLoading(false);
      }
    })();
  }, [eventId, eventIdParam]);

  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);
  const teamLabel = (id: number | null) => teamLabelFromMap(teamsById, id);

  const homeName = event ? teamLabel(event.home_team_id) : "";
  const awayName = event ? teamLabel(event.away_team_id) : "";

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
        // 404s here are expected when the model hasn't scored this game yet,
        // so we treat them as a soft error and just show a friendly message.
        if (process.env.NODE_ENV === "development") {
          console.warn("Prediction API error", err);
        }
        const message = parseApiError(
          err,
          "No model prediction is available yet for this game.",
          "Failed to load prediction",
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
        // 404s here are also expected when we haven't generated insights yet.
        if (process.env.NODE_ENV === "development") {
          console.warn("Insights API error", err);
        }
        const message = parseApiError(
          err,
          "No insights are available yet for this game.",
          "Failed to load insights",
        );
        setInsightsError(message);
      } finally {
        setInsightsLoading(false);
      }
    })();
  }, [event]);

  const fallbackModelKey = "nba_logreg_b2b_v1";

  const edge =
    isValidProb(prediction?.p_home) && isValidProb(prediction?.p_away)
      ? Math.abs(prediction!.p_home - prediction!.p_away)
      : null;

  const edgeWidthPct =
    edge !== null ? Math.min(Math.max(edge * 200, 5), 100) : 0;

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

  // ---------- Render ----------

  return (
    <main className="min-h-screen bg-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-5xl space-y-6">
        {/* Back link */}
        <div className="flex items-center justify-between">
          <Link
            href="/games"
            className="text-xs text-zinc-400 hover:text-zinc-200 underline underline-offset-4"
          >
            ← Back to games
          </Link>
        </div>

        {/* Loading / error states */}
        {loading && (
          <p className="text-sm text-zinc-500">Loading game details…</p>
        )}

        {!loading && error && (
          <div className="rounded-xl border border-red-800 bg-red-950/40 px-4 py-3">
            <p className="text-sm text-red-300">{error}</p>
          </div>
        )}

        {!loading && !error && !event && (
          <div className="rounded-xl border border-zinc-800 bg-zinc-950/50 px-4 py-3">
            <p className="text-sm text-zinc-300">
              This game could not be found in the schedule.
            </p>
          </div>
        )}

        {/* Main content when we have an event */}
        {!loading && !error && event && (
          <>
            {/* =================================== */}
            {/* 1. MATCHUP HEADER / SUMMARY CARD   */}
            {/* =================================== */}
            <section className="rounded-3xl border border-zinc-800 bg-gradient-to-br from-zinc-950 via-zinc-900 to-black px-5 py-5 sm:px-7 sm:py-6 shadow-lg shadow-black/40 space-y-4">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                {/* Matchup + league/date */}
                <div className="flex items-start gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-zinc-900 border border-zinc-700 text-xl">
                    {sportIconFromId(event.sport_id)}
                  </div>
                  <div>
                    <h1 className="text-xl sm:text-2xl font-semibold text-zinc-50 tracking-tight">
                      {awayName}{" "}
                      <span className="text-zinc-500">@</span> {homeName}
                    </h1>
                    <p className="mt-1 text-xs sm:text-sm text-zinc-400">
                      {sportLabelFromId(event.sport_id)} · {event.date}
                    </p>
                  </div>
                </div>

                {/* Meta: Game ID + Model + Edge */}
                <div className="flex flex-col items-start sm:items-end gap-2 text-[11px]">
                  <div className="flex flex-wrap gap-2 justify-end">
                    <span className="inline-flex items-center rounded-full border border-zinc-700 bg-zinc-900/70 px-2.5 py-1 uppercase tracking-[0.16em] text-[10px] text-zinc-300">
                      GAME ID&nbsp;
                      <span className="font-mono text-zinc-100">
                        {event.event_id}
                      </span>
                    </span>
                    {prediction && (
                      <span className="inline-flex items-center rounded-full border border-zinc-700 bg-zinc-900/70 px-2.5 py-1 uppercase tracking-[0.16em] text-[10px] text-zinc-300">
                        MODEL&nbsp;
                        <span className="font-mono text-zinc-100">
                          {fallbackModelKey}
                        </span>
                      </span>
                    )}
                  </div>

                  {edgeCategory && (
                    <span
                      className={
                        "inline-flex items-center rounded-full px-2.5 py-1 uppercase tracking-[0.16em] text-[10px]" +
                        (edgeCategory === "STRONG FAVORITE"
                          ? " bg-emerald-500/10 text-emerald-300 border border-emerald-500/60"
                          : edgeCategory === "MODEST EDGE"
                            ? " bg-amber-500/10 text-amber-300 border border-amber-500/60"
                            : " bg-zinc-800 text-zinc-200 border border-zinc-600")
                      }
                    >
                      Edge: {edgeCategory}
                    </span>
                  )}
                </div>
              </div>

              {/* Confidence bar (redesigned) */}
              {edge !== null && edgeCategory && (
                <div className="space-y-1.5">
                  <div className="flex items-center justify-between text-[11px] text-zinc-400">
                    <span>Model confidence</span>
                    <span className="font-medium text-zinc-100">
                      {edgeCategory}
                    </span>
                  </div>
                  <div className="relative h-2 rounded-full bg-zinc-800 overflow-hidden">
                    <div
                      className={`absolute left-0 top-0 h-full ${edgeFillClass} bg-gradient-to-r from-transparent via-current to-current/60 transition-all`}
                      style={{ width: `${edgeWidthPct}%` }}
                    />
                    <div className="absolute inset-0 pointer-events-none bg-gradient-to-r from-transparent via-white/5 to-transparent" />
                  </div>
                  <p className="text-[10px] text-zinc-500">
                    Larger bar = bigger gap between home and away win
                    probabilities.
                  </p>
                </div>
              )}
            </section>

            {/* =================================== */}
            {/* 2. FINAL SCORE (IF COMPLETED GAME) */}
            {/* =================================== */}
            {isFinal && (
              <section className="rounded-2xl border border-zinc-800 bg-zinc-950/80 px-5 py-4 sm:px-6 sm:py-4 space-y-2">
                <div className="flex items-center justify-between">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                    Final score
                  </div>
                  <span className="inline-flex items-center rounded-full border border-zinc-700 bg-zinc-900/90 px-2.5 py-0.5 text-[10px] uppercase tracking-[0.16em] text-zinc-200">
                    {event.home_win ? "Home win" : "Away win"}
                  </span>
                </div>
                <div className="flex items-baseline justify-between text-sm sm:text-base">
                  <span className="font-medium text-zinc-100">
                    {awayName}{" "}
                    <span className="font-mono">{event.away_score}</span> @{" "}
                    {homeName}{" "}
                    <span className="font-mono">{event.home_score}</span>
                  </span>
                </div>
              </section>
            )}

            {/* =================================== */}
            {/* 3. PREDICTION + INSIGHTS GRID       */}
            {/* =================================== */}
            <section className="grid gap-4 md:grid-cols-[1.4fr,1fr]">
              {/* =============================== */}
              {/* 3A. MODEL PREDICTION PANEL      */}
              {/* =============================== */}
              <div className="rounded-2xl border border-zinc-800 bg-zinc-950/80 px-5 py-5 space-y-4 shadow-sm shadow-black/40">
                <div className="flex items-center justify-between mb-1">
                  <div>
                    <h2 className="text-sm font-semibold text-zinc-100">
                      Model prediction
                    </h2>
                    <p className="text-[11px] text-zinc-500">
                      Win probabilities from your baseline NBA model.
                    </p>
                  </div>
                  <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                    POST /predict/nba
                  </span>
                </div>

                {predLoading && (
                  <p className="text-xs text-zinc-500">
                    Loading model prediction…
                  </p>
                )}

                {!predLoading && predError && (
                  <div className="rounded-lg border border-zinc-800 bg-zinc-950/90 px-3 py-2">
                    <p className="text-xs text-zinc-300">{predError}</p>
                  </div>
                )}

                {!predLoading && !predError && !prediction && (
                  <p className="text-xs text-zinc-500">
                    No prediction yet. Try again after the model has scored this
                    game.
                  </p>
                )}

                {/* Symmetric Away / Home cards */}
                {!predLoading && !predError && prediction && (
                  <>
                    {/* --- major redesign: symmetric side-by-side tiles --- */}
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                      {/* Away side */}
                      <button
                        type="button"
                        onClick={() =>
                          setSelectedSide((prev) =>
                            prev === "away" ? null : "away",
                          )
                        }
                        className={
                          "rounded-2xl border px-4 py-3 text-left transition-colors shadow-sm " +
                          (selectedSide === "away"
                            ? "border-blue-400 bg-blue-500/10 shadow-blue-500/20"
                            : "border-zinc-800 bg-zinc-950/90 hover:border-zinc-700")
                        }
                      >
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-[11px] text-zinc-500 uppercase tracking-[0.16em]">
                            Away
                          </span>
                          {selectedSide === "away" && (
                            <span className="text-[10px] text-blue-300">
                              Selected
                            </span>
                          )}
                        </div>
                        <div className="text-sm sm:text-base font-semibold text-zinc-50">
                          {prediction.away_team || awayName || "Away"}
                        </div>
                        <div className="mt-2 flex items-center justify-between text-[11px] text-zinc-400">
                          <span>
                            Win prob:{" "}
                            <span className="text-zinc-100 font-semibold">
                              {safePercent(prediction.p_away)}
                            </span>
                          </span>
                          <span>
                            Implied:{" "}
                            <span className="font-mono">
                              {impliedOdds(prediction.p_away)}
                            </span>
                          </span>
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
                          "rounded-2xl border px-4 py-3 text-left transition-colors shadow-sm " +
                          (selectedSide === "home"
                            ? "border-blue-400 bg-blue-500/10 shadow-blue-500/20"
                            : "border-zinc-800 bg-zinc-950/90 hover:border-zinc-700")
                        }
                      >
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-[11px] text-zinc-500 uppercase tracking-[0.16em]">
                            Home
                          </span>
                          {selectedSide === "home" && (
                            <span className="text-[10px] text-blue-300">
                              Selected
                            </span>
                          )}
                        </div>
                        <div className="text-sm sm:text-base font-semibold text-zinc-50">
                          {prediction.home_team || homeName || "Home"}
                        </div>
                        <div className="mt-2 flex items-center justify-between text-[11px] text-zinc-400">
                          <span>
                            Win prob:{" "}
                            <span className="text-zinc-100 font-semibold">
                              {safePercent(prediction.p_home)}
                            </span>
                          </span>
                          <span>
                            Implied:{" "}
                            <span className="font-mono">
                              {impliedOdds(prediction.p_home)}
                            </span>
                          </span>
                        </div>
                      </button>
                    </div>

                    {/* Metadata under tiles */}
                    <div className="mt-3 text-[11px] text-zinc-500 flex flex-wrap items-center gap-2">
                      <span>
                        Generated at{" "}
                        <span className="text-zinc-300">
                          {new Date(generatedAt).toLocaleString()}
                        </span>
                      </span>
                      <span className="hidden sm:inline">·</span>
                      <span>
                        Model:{" "}
                        <span className="font-mono text-zinc-200">
                          {fallbackModelKey}
                        </span>
                      </span>
                    </div>

                    {/* Outcome vs. model block */}
                    {isFinal && predictionOutcome && (
                      <div className="mt-4 rounded-2xl border border-zinc-800 bg-zinc-900/80 px-4 py-3 text-[11px] text-zinc-300 space-y-1.5">
                        <div className="flex items-center justify-between">
                          <span className="uppercase tracking-[0.16em] text-[10px] text-zinc-500">
                            Outcome vs model
                          </span>
                          <span
                            className={
                              "inline-flex items-center rounded-full px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] " +
                              (predictionOutcome.correct
                                ? "bg-emerald-500/10 text-emerald-300 border border-emerald-500/60"
                                : "bg-red-500/10 text-red-300 border border-red-500/60")
                            }
                          >
                            {predictionOutcome.correct
                              ? "Model was correct"
                              : "Model was wrong"}
                          </span>
                        </div>
                        <p className="text-[11px] text-zinc-400">
                          Pre-game, the model favored{" "}
                          <span className="text-zinc-100 font-medium">
                            {predictionOutcome.favoredSide === "home"
                              ? homeName
                              : awayName}
                          </span>
                          .
                        </p>
                      </div>
                    )}
                  </>
                )}
              </div>

              {/* =============================== */}
              {/* 3B. INSIGHTS / EXPLANATION CARD */}
              {/* =============================== */}
              <div className="rounded-2xl border border-zinc-800 bg-zinc-950/80 px-5 py-5 space-y-4 shadow-sm shadow-black/40">
                <div className="flex items-center justify-between mb-1">
                  <div>
                    <h2 className="text-sm font-semibold text-zinc-100">
                      Why the model likes this side
                    </h2>
                    <p className="text-[11px] text-zinc-500">
                      Top SHAP-style drivers and full narrative explanation.
                    </p>
                  </div>
                  <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                    GET /insights/nba/{"{event_id}"}
                  </span>
                </div>

                {insightsLoading && (
                  <p className="text-xs text-zinc-500">
                    Loading insights for this game…
                  </p>
                )}

                {!insightsLoading && insightsError && (
                  <div className="rounded-lg border border-zinc-800 bg-zinc-950/90 px-3 py-2">
                    <p className="text-xs text-zinc-300">{insightsError}</p>
                  </div>
                )}

                {!insightsLoading &&
                  !insightsError &&
                  (!insights || insights.length === 0) && (
                    <p className="text-xs text-zinc-500">
                      No insights are available yet for this game.
                    </p>
                  )}

                {!insightsLoading &&
                  !insightsError &&
                  insights &&
                  insights.length > 0 && (
                    <div className="space-y-4">
                      {/* --- KEY FACTORS block (highlighted subset) --- */}
                      {keyFactors.length > 0 && (
                        <div className="space-y-2">
                          <div className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
                            Key factors
                          </div>
                          <ul className="space-y-1.5">
                            {keyFactors.map((ins, idx) => (
                              <li
                                key={`${ins.type}-${ins.label}-${idx}`}
                                className="flex items-center justify-between gap-2 rounded-xl border border-zinc-800 bg-zinc-950/90 px-3 py-2"
                              >
                                <div className="text-[11px] text-zinc-100">
                                  {ins.label}
                                  <span className="block text-[11px] text-zinc-500">
                                    {ins.detail}
                                  </span>
                                </div>
                                {typeof ins.value === "number" && (
                                  <span className="text-[10px] text-zinc-300 font-mono">
                                    {(ins.value * 100).toFixed(1)}%
                                  </span>
                                )}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}

                      {/* --- FULL EXPLANATION block --- */}
                      <div className="border-t border-zinc-800 pt-3">
                        <div className="text-[11px] uppercase tracking-[0.16em] text-zinc-500 mb-2">
                          Full explanation
                        </div>
                        <ul className="space-y-1.5 max-h-52 overflow-y-auto pr-1">
                          {insights.map((ins, idx) => (
                            <li
                              key={`${ins.type}-${ins.label}-full-${idx}`}
                              className="text-[11px] text-zinc-300"
                            >
                              <span className="font-medium text-zinc-100">
                                {ins.label}
                              </span>
                              {typeof ins.value === "number" && (
                                <span className="ml-1 text-[10px] text-zinc-400 font-mono">
                                  {(ins.value * 100).toFixed(1)}%
                                </span>
                              )}
                              <span className="block text-zinc-400">
                                {ins.detail}
                              </span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  )}
              </div>
            </section>
          </>
        )}
      </div>
    </main>
  );
}