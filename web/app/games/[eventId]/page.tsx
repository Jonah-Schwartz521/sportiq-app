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

  // ---------- Derived helpers ----------

  const edgeCategory = useMemo(() => {
    if (!prediction) return null;
    const edge = Math.abs(prediction.p_home - prediction.p_away);
    if (edge < 0.05) return "COIN FLIP";
    if (edge < 0.15) return "MODEST EDGE";
    return "STRONG FAVORITE";
  }, [prediction]);

  const keyFactors = useMemo(() => {
    if (!insights || insights.length === 0) return [];
    const sorted = [...insights].sort((a, b) => {
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
      } catch (error: unknown) {
        console.error(error);
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
      } catch (error: unknown) {
        console.error(error);
        const message =
          error instanceof Error
            ? error.message
            : "Failed to load prediction";
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
      } catch (error: unknown) {
        console.error(error);
        const message =
          error instanceof Error ? error.message : "Failed to load insights";
        setInsightsError(message);
      } finally {
        setInsightsLoading(false);
      }
    })();
  }, [event]);

  const fallbackModelKey = "nba_logreg_b2b_v1";

  return (
    <main className="min-h-screen bg-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-4xl space-y-6">
        {/* Header */}
        <header className="flex items-center justify-between gap-2">
          <Link
            href="/games"
            className="text-xs text-zinc-400 hover:text-zinc-200 transition-colors"
          >
            ← Back to games
          </Link>

          <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
            Game Detail
          </span>
        </header>

        {/* Errors / loading */}
        {loading && <p className="text-sm text-zinc-500">Loading game…</p>}
        {error && !loading && <p className="text-sm text-red-400">{error}</p>}
        {!loading && !error && !event && (
          <p className="text-sm text-zinc-500">Game not found.</p>
        )}

        {event && (
          <div className="space-y-6">
            {/* ---- MATCHUP HEADER ---- */}
            <section className="space-y-2">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-2 text-xs text-zinc-400">
                  <span>{sportIconFromId(event.sport_id)}</span>
                  <span className="uppercase tracking-[0.16em]">
                    {sportLabelFromId(event.sport_id)}
                  </span>
                  <span className="inline-flex items-center rounded-full border border-zinc-700 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em]">
                    {event.status || "scheduled"}
                  </span>
                </div>

                <div className="flex items-center gap-2">
                  {edgeCategory && (
                    <span className="inline-flex items-center rounded-full bg-zinc-900 border border-zinc-700 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] text-zinc-100">
                      {edgeCategory}
                    </span>
                  )}
                  <span className="text-[10px] text-zinc-500">
                    Event ID: {event.event_id}
                  </span>
                </div>
              </div>

              <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
                <span className="flex flex-col sm:flex-row sm:items-baseline gap-1 sm:gap-2">
                  <span className="truncate">{awayName}</span>
                  <span className="text-zinc-500 text-base sm:text-lg">@</span>
                  <span className="truncate">{homeName}</span>
                </span>
              </h1>

              <p className="text-xs text-zinc-400">
                {event.date} · {event.venue || "TBD"}
              </p>
            </section>

            {/* ---- PREDICTION PANEL ---- */}
            <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 p-4 space-y-3">
              <div className="flex items-center justify-between">
                <h2 className="text-sm font-semibold text-zinc-100">
                  Model Prediction
                </h2>
                <div className="text-[10px] text-zinc-500 text-right space-y-0.5">
                  <div>
                    Model:{" "}
                    <span className="font-mono">{fallbackModelKey}</span>
                  </div>
                  <div>Generated: {generatedAt}</div>
                </div>
              </div>

              {predLoading && (
                <p className="text-xs text-zinc-500">Loading prediction…</p>
              )}
              {predError && !predLoading && (
                <p className="text-xs text-red-400">{predError}</p>
              )}

              {prediction && !predLoading && !predError && (
                <>
                  <div className="rounded-xl bg-zinc-900/60 border border-zinc-800 px-3 py-2 text-xs text-zinc-200 flex flex-col gap-2">
                    <div className="flex justify-between">
                      <span className="text-zinc-400">
                        {homeName || "Home"} win prob
                      </span>
                      <span className="font-medium">
                        {(prediction.p_home * 100).toFixed(1)}%
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-400">
                        {awayName || "Away"} win prob
                      </span>
                      <span className="font-medium">
                        {(prediction.p_away * 100).toFixed(1)}%
                      </span>
                    </div>
                  </div>

                  {/* Quick Bet */}
                  <div className="mt-3 border-t border-zinc-800 pt-3 space-y-2">
                    <div className="flex items-center justify-between">
                      <h3 className="text-xs font-semibold text-zinc-100">
                        Quick Bet (demo)
                      </h3>
                      <p className="text-[10px] text-zinc-500">
                        Select a side to see implied odds
                      </p>
                    </div>

                    <div className="flex flex-col sm:flex-row gap-2">
                      <button
                        type="button"
                        onClick={() => setSelectedSide("home")}
                        className={
                          "flex-1 rounded-lg border px-3 py-2 text-xs text-left " +
                          (selectedSide === "home"
                            ? "border-emerald-500/70 bg-emerald-500/10"
                            : "border-zinc-700 bg-zinc-900/40 hover:border-zinc-500")
                        }
                      >
                        <div className="flex justify-between gap-2">
                          <span className="font-medium truncate">
                            {homeName || "Home"}
                          </span>
                          <span className="text-[11px] text-zinc-400 text-right">
                            {(prediction.p_home * 100).toFixed(1)}% ·{" "}
                            {impliedOdds(prediction.p_home)}
                          </span>
                        </div>
                      </button>

                      <button
                        type="button"
                        onClick={() => setSelectedSide("away")}
                        className={
                          "flex-1 rounded-lg border px-3 py-2 text-xs text-left " +
                          (selectedSide === "away"
                            ? "border-emerald-500/70 bg-emerald-500/10"
                            : "border-zinc-700 bg-zinc-900/40 hover:border-zinc-500")
                        }
                      >
                        <div className="flex justify-between gap-2">
                          <span className="font-medium truncate">
                            {awayName || "Away"}
                          </span>
                          <span className="text-[11px] text-zinc-400 text-right">
                            {(prediction.p_away * 100).toFixed(1)}% ·{" "}
                            {impliedOdds(prediction.p_away)}
                          </span>
                        </div>
                      </button>
                    </div>

                    {selectedSide && prediction && (
                      <p className="text-[11px] text-zinc-400">
                        You&apos;ve selected{" "}
                        <span className="text-zinc-100 font-medium">
                          {selectedSide === "home"
                            ? homeName || "Home"
                            : awayName || "Away"}
                        </span>{" "}
                        with implied odds of{" "}
                        <span className="text-zinc-100 font-mono">
                          {selectedSide === "home"
                            ? impliedOdds(prediction.p_home)
                            : impliedOdds(prediction.p_away)}
                        </span>
                        . This is a demo only – no real bets placed.
                      </p>
                    )}
                  </div>
                </>
              )}
            </section>

            {/* ---- INSIGHTS PANEL ---- */}
            <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 p-4 space-y-4">
              <h2 className="text-sm font-semibold text-zinc-100">Insights</h2>

              {insightsLoading && (
                <p className="text-xs text-zinc-500">Loading insights…</p>
              )}
              {insightsError && !insightsLoading && (
                <p className="text-xs text-red-400">{insightsError}</p>
              )}

              {insights &&
                insights.length > 0 &&
                !insightsLoading &&
                !insightsError && (
                  <div className="space-y-4">
                    {/* Key Factors */}
                    {keyFactors.length > 0 && (
                      <div className="rounded-xl bg-zinc-900/70 border border-zinc-800 px-3 py-2">
                        <h3 className="text-[11px] font-semibold text-zinc-100 mb-1">
                          Key Factors (Top 3)
                        </h3>
                        <ul className="space-y-1.5">
                          {keyFactors.map((ins, idx) => (
                            <li key={`kf-${idx}`}>
                              <div className="flex justify-between gap-2">
                                <span className="font-medium text-zinc-100">
                                  {ins.label}
                                </span>
                                {ins.value != null && (
                                  <span className="text-[11px] text-zinc-400">
                                    {(ins.value * 100).toFixed(1)}%
                                  </span>
                                )}
                              </div>
                              <p className="text-[11px] text-zinc-400">
                                {ins.detail}
                              </p>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {/* Full list */}
                    <ul className="space-y-2 text-xs">
                      {insights.map((insight, idx) => (
                        <li key={idx} className="space-y-0.5">
                          <div className="flex justify-between gap-2">
                            <span className="font-medium text-zinc-100">
                              {insight.label}
                            </span>
                            {insight.value != null && (
                              <span className="text-[11px] text-zinc-400">
                                {(insight.value * 100).toFixed(1)}%
                              </span>
                            )}
                          </div>
                          <p className="text-[11px] text-zinc-400">
                            {insight.detail}
                          </p>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

              {(!insights || insights.length === 0) &&
                !insightsLoading &&
                !insightsError && (
                  <p className="text-xs text-zinc-400">
                    No insights available yet for this matchup.
                  </p>
                )}
            </section>

            {/* ---- EVENT INFO ---- */}
            <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 p-4">
              <h2 className="text-sm font-semibold text-zinc-100">
                Event Info
              </h2>

              <dl className="grid grid-cols-1 sm:grid-cols-2 gap-y-2 sm:gap-x-8 text-xs text-zinc-300 mt-2">
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Sport</dt>
                  <dd>{sportLabelFromId(event.sport_id)}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Status</dt>
                  <dd>{event.status || "scheduled"}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Home</dt>
                  <dd>{homeName}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Away</dt>
                  <dd>{awayName}</dd>
                </div>
              </dl>
            </section>

            {/* ---- RAW JSON ---- */}
            <section className="rounded-2xl border border-zinc-900 bg-zinc-950/80 p-4">
              <details className="text-xs text-zinc-400">
                <summary className="cursor-pointer mb-1">
                  Raw event JSON (debug)
                </summary>
                <pre className="mt-2 whitespace-pre-wrap break-all text-[10px]">
                  {JSON.stringify(event, null, 2)}
                </pre>
              </details>
            </section>
          </div>
        )}
      </div>
    </main>
  );
}