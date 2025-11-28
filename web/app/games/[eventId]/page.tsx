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
            {/* Header: matchup + basics */}
            <section className="rounded-2xl border border-zinc-800 bg-gradient-to-br from-zinc-950 to-zinc-900 px-4 py-4 sm:px-6 sm:py-5 space-y-3">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <span className="text-xl">
                    {sportIconFromId(event.sport_id)}
                  </span>
                  <div>
                    <h1 className="text-lg sm:text-xl font-semibold text-zinc-50">
                      {awayName} @ {homeName}
                    </h1>
                    <p className="text-xs text-zinc-400">
                      {sportLabelFromId(event.sport_id)} · {event.date}
                    </p>
                  </div>
                </div>

                <div className="hidden sm:flex flex-col items-end text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                  <span>GAME ID {event.event_id}</span>
                  {prediction && (
                    <span>MODEL {fallbackModelKey}</span>
                  )}
                </div>
              </div>

              {/* Confidence bar */}
              {edge !== null && edgeCategory && (
                <div className="mt-2">
                  <div className="flex items-center justify-between text-[11px] text-zinc-400 mb-1">
                    <span>Model confidence</span>
                    <span className="font-medium text-zinc-200">
                      {edgeCategory}
                    </span>
                  </div>
                  <div className="h-1.5 rounded-full bg-zinc-800 overflow-hidden">
                    <div
                      className={`h-full ${edgeFillClass} transition-all`}
                      style={{ width: `${edgeWidthPct}%` }}
                    />
                  </div>
                </div>
              )}
            </section>

            {/* Final score summary for completed games */}
            {isFinal && (
              <section className="rounded-2xl border border-zinc-800 bg-zinc-950/70 px-4 py-3 sm:px-6 sm:py-4 space-y-2">
                <div className="flex items-center justify-between">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                    Final score
                  </div>
                  <span className="inline-flex items-center rounded-full border border-zinc-700 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] text-zinc-300">
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

            {/* Prediction + insights grid */}
            <section className="grid gap-4 md:grid-cols-[1.4fr,1fr]">
              {/* Prediction card */}
              <div className="rounded-2xl border border-zinc-800 bg-zinc-950/70 px-4 py-4 sm:px-5 sm:py-5 space-y-3">
                <div className="flex items-center justify-between mb-1">
                  <div>
                    <h2 className="text-sm font-semibold text-zinc-100">
                      Model prediction
                    </h2>
                    <p className="text-[11px] text-zinc-500">
                      Probabilities are generated by your NBA baseline model.
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
                  <div className="rounded-lg border border-zinc-800 bg-zinc-950/80 px-3 py-2">
                    <p className="text-xs text-zinc-300">{predError}</p>
                  </div>
                )}

                {!predLoading && !predError && !prediction && (
                  <p className="text-xs text-zinc-500">
                    No prediction yet. Try again after the model has scored this
                    game.
                  </p>
                )}

                {!predLoading && !predError && prediction && (
                  <>
                    <div className="grid grid-cols-2 gap-3">
                      {/* Away side */}
                      <button
                        type="button"
                        onClick={() =>
                          setSelectedSide((prev) =>
                            prev === "away" ? null : "away",
                          )
                        }
                        className={
                          "rounded-xl border px-3 py-2 text-left transition-colors " +
                          (selectedSide === "away"
                            ? "border-blue-400 bg-blue-500/10"
                            : "border-zinc-800 bg-zinc-950/80 hover:border-zinc-700")
                        }
                      >
                        <div className="text-[11px] text-zinc-500 uppercase tracking-[0.16em]">
                          Away
                        </div>
                        <div className="text-sm font-semibold text-zinc-50">
                          {prediction.away_team || awayName || "Away"}
                        </div>
                        <div className="mt-1 text-[11px] text-zinc-400">
                          Win prob:{" "}
                          <span className="text-zinc-100">
                            {safePercent(prediction.p_away)}
                          </span>
                        </div>
                        <div className="text-[10px] text-zinc-500">
                          Implied odds:{" "}
                          <span className="font-mono">
                            {impliedOdds(prediction.p_away)}
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
                          "rounded-xl border px-3 py-2 text-left transition-colors " +
                          (selectedSide === "home"
                            ? "border-blue-400 bg-blue-500/10"
                            : "border-zinc-800 bg-zinc-950/80 hover:border-zinc-700")
                        }
                      >
                        <div className="text-[11px] text-zinc-500 uppercase tracking-[0.16em]">
                          Home
                        </div>
                        <div className="text-sm font-semibold text-zinc-50">
                          {prediction.home_team || homeName || "Home"}
                        </div>
                        <div className="mt-1 text-[11px] text-zinc-400">
                          Win prob:{" "}
                          <span className="text-zinc-100">
                            {safePercent(prediction.p_home)}
                          </span>
                        </div>
                        <div className="text-[10px] text-zinc-500">
                          Implied odds:{" "}
                          <span className="font-mono">
                            {impliedOdds(prediction.p_home)}
                          </span>
                        </div>
                      </button>
                    </div>

                    <div className="mt-2 text-[11px] text-zinc-500 flex flex-wrap items-center gap-2">
                      <span>
                        Generated at{" "}
                        <span className="text-zinc-300">
                          {new Date(generatedAt).toLocaleString()}
                        </span>
                      </span>
                      <span className="hidden sm:inline">·</span>
                      <span>Model: {fallbackModelKey}</span>
                    </div>
                    {isFinal && predictionOutcome && (
                      <div className="mt-3 rounded-lg border border-zinc-800 bg-zinc-900/80 px-3 py-2 text-[11px] text-zinc-300">
                        <div className="flex items-center justify-between mb-1">
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
                            {predictionOutcome.favoredSide === "home" ? homeName : awayName}
                          </span>
                          .
                        </p>
                      </div>
                    )}
                  </>
                )}
              </div>

              {/* Insights card */}
              <div className="rounded-2xl border border-zinc-800 bg-zinc-950/70 px-4 py-4 sm:px-5 sm:py-5 space-y-3">
                <div className="flex items-center justify-between mb-1">
                  <div>
                    <h2 className="text-sm font-semibold text-zinc-100">
                      Why the model likes this side
                    </h2>
                    <p className="text-[11px] text-zinc-500">
                      Top drivers pulled from your SHAP-based insights.
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
                  <div className="rounded-lg border border-zinc-800 bg-zinc-950/80 px-3 py-2">
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
                    <div className="space-y-3">
                      {keyFactors.length > 0 && (
                        <div className="space-y-1">
                          <div className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
                            Key factors
                          </div>
                          <ul className="space-y-1.5">
                            {keyFactors.map((ins, idx) => (
                              <li
                                key={`${ins.type}-${ins.label}-${idx}`}
                                className="flex items-start justify-between gap-2"
                              >
                                <div className="text-[11px] text-zinc-200">
                                  {ins.label}
                                  <span className="block text-[11px] text-zinc-500">
                                    {ins.detail}
                                  </span>
                                </div>
                                {typeof ins.value === "number" && (
                                  <span className="text-[10px] text-zinc-400 font-mono">
                                    {(ins.value * 100).toFixed(1)}%
                                  </span>
                                )}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}

                      <div className="border-t border-zinc-800 pt-2 mt-2">
                        <div className="text-[11px] uppercase tracking-[0.16em] text-zinc-500 mb-1">
                          Full explanation
                        </div>
                        <ul className="space-y-1.5 max-h-48 overflow-y-auto pr-1">
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