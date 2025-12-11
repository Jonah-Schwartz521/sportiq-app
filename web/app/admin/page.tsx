"use client";

import { useEffect, useMemo, useState } from "react";
import { api, type Team, type Event, type Metrics } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";
import PredictPanel from "@/components/PredictPanel";
import PredictionsPanel from "@/components/Predictions_Panel";
import InsightsPanel from "@/components/InsightsPanel";

export default function Home() {
  const [health, setHealth] = useState("checking...");
  const [teams, setTeams] = useState<Team[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [metricsError, setMetricsError] = useState<string | null>(null);

  // 🔹 Load model metrics (accuracy, Brier score, etc.) – logic unchanged
  useEffect(() => {
    (async () => {
      try {
        const m = await api.metrics();
        setMetrics(m);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load model metrics";
        setMetricsError(message);
      }
    })();
  }, []);

  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);
  const [selectedEventId, setSelectedEventId] = useState<number | null>(null);
  const [loadingEventDetail, setLoadingEventDetail] = useState(false);
  const [eventDetailError, setEventDetailError] = useState<string | null>(null);

  const [showAll, setShowAll] = useState(false);
  const MAX_PREVIEW = 20;

  // Load health, teams, events – logic unchanged
  useEffect(() => {
    (async () => {
      try {
        const [h, t, e] = await Promise.all([
          api.health(),
          api.teams(),
          api.events(),
        ]);

        setHealth(h.status);
        setTeams(t.items || []);
        setEvents(e.items || []);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load admin data";
        setError(message);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const visibleEvents = useMemo(() => {
    if (!events) return [];
    return showAll ? events : events.slice(0, MAX_PREVIEW);
  }, [events, showAll]);

  // 🔹 Build team lookup + label helper (shared pattern with /games) – logic unchanged
  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);

  function teamLabel(id: number | null): string {
    return teamLabelFromMap(teamsById, id);
  }

  // Click handler for "View" – logic unchanged
  async function handleViewEvent(eventId: number) {
    try {
      setSelectedEvent(null);
      setSelectedEventId(eventId);
      setEventDetailError(null);
      setLoadingEventDetail(true);

      const data = await api.eventById(eventId);
      setSelectedEvent(data);
    } catch (err: unknown) {
      console.error(err);
      const msg =
        err instanceof Error ? err.message : "Failed to load event details";
      setEventDetailError(msg);
    } finally {
      setLoadingEventDetail(false);
    }
  }

  return (
    // 🌌 Global admin background & layout: darker gradient + centered content
    <main className="min-h-screen bg-gradient-to-b from-black via-slate-950 to-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-6xl space-y-8">
        {/* ========================== */}
        {/* 1. PAGE HEADER             */}
        {/* ========================== */}
        {/* Header: more breathing room, soft bottom divider, subtle subtitle */}
        <section className="pb-4 border-b border-slate-800/80">
          <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h1 className="text-3xl sm:text-4xl font-semibold tracking-tight">
                SportIQ Admin Surface
              </h1>
              <p className="mt-2 text-xs sm:text-sm text-zinc-500 max-w-xl">
                Internal control panel for your models and data pipelines.
              </p>
            </div>
            <p className="text-[11px] sm:text-xs text-zinc-500">
              <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/70 px-3 py-1 border border-zinc-800">
                <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
                Backend contracts: <span className="font-mono">/teams</span>,{" "}
                <span className="font-mono">/events</span>,{" "}
                <span className="font-mono">/predict</span>,{" "}
                <span className="font-mono">/predictions</span>,{" "}
                <span className="font-mono">/insights</span>
              </span>
            </p>
          </header>
        </section>

        {/* ========================== */}
        {/* 2. SYSTEM HEALTH / METRICS */}
        {/* ========================== */}
        {/* Unified card for health + metrics with icons + better hierarchy */}
        <section className="rounded-2xl border border-slate-800 bg-slate-950/70 px-6 py-5 shadow-sm shadow-black/40">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-emerald-500/10 border border-emerald-500/40">
                <span className="text-emerald-400 text-lg">●</span>
              </div>
              <div>
                <h2 className="text-sm font-semibold text-slate-100">
                  System health
                </h2>
                <p className="text-xs text-zinc-500">
                  API uptime and live model performance.
                </p>
              </div>
            </div>

            <div className="flex items-center gap-2 text-xs">
              {error ? (
                <span className="inline-flex items-center gap-1 rounded-full border border-red-500/60 bg-red-500/10 px-2 py-1 text-red-300">
                  <span className="h-1.5 w-1.5 rounded-full bg-red-400" />
                  API issue
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/60 bg-emerald-500/10 px-2 py-1 text-emerald-300">
                  <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
                  {loading ? "Checking..." : "API ok"}
                </span>
              )}
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-[1.4fr,2fr]">
            {/* API health block */}
            <div className="rounded-xl bg-black/60 border border-slate-900/80 px-4 py-3 space-y-2">
              <div className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
                API health
              </div>

              {error ? (
                <div className="text-sm text-red-400">{error}</div>
              ) : (
                <div className="flex items-center justify-between text-sm">
                  <div>
                    <span className="text-zinc-300">/health status</span>
                    <span className="mx-1 text-zinc-600">→</span>
                    <span
                      className={
                        health === "ok"
                          ? "text-emerald-400 font-medium"
                          : "text-yellow-400"
                      }
                    >
                      {loading ? "loading..." : health}
                    </span>
                  </div>
                </div>
              )}
            </div>

            {/* Model metrics block */}
            <div className="rounded-xl bg-black/60 border border-slate-900/80 px-4 py-3">
              <div className="flex items-center justify-between mb-2">
                <div className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
                  Model metrics
                </div>
                <span className="text-[10px] text-zinc-500">
                  /metrics · nba_logreg_b2b_v1
                </span>
              </div>

              {metrics ? (
                <div className="grid grid-cols-3 gap-3 text-xs text-zinc-300">
                  <div className="space-y-1">
                    <div className="flex items-center gap-1 text-[10px] text-zinc-500 uppercase">
                      <span>📊</span>
                      <span>Games evaluated</span>
                    </div>
                    <div className="font-mono text-sm">
                      {metrics.num_games.toLocaleString()}
                    </div>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-1 text-[10px] text-zinc-500 uppercase">
                      <span>✅</span>
                      <span>Accuracy</span>
                    </div>
                    <div className="font-mono text-sm">
                      {(metrics.accuracy * 100).toFixed(1)}%
                    </div>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-1 text-[10px] text-zinc-500 uppercase">
                      <span>📉</span>
                      <span>Brier score</span>
                    </div>
                    <div className="font-mono text-sm">
                      {metrics.brier_score.toFixed(3)}
                    </div>
                  </div>
                </div>
              ) : metricsError ? (
                <p className="mt-2 text-xs text-zinc-500">
                  Metrics unavailable: {metricsError}
                </p>
              ) : (
                <p className="mt-2 text-xs text-zinc-500">
                  Loading model metrics…
                </p>
              )}
            </div>
          </div>
        </section>

        {/* ================================ */}
        {/* 3. SAMPLE TEAMS + SAMPLE EVENTS  */}
        {/* ================================ */}
        {/* Paired cards: teams + events with scrollable bodies */}
        <section className="grid gap-5 md:grid-cols-2">
          {/* TEAMS CARD */}
          <div className="rounded-2xl border border-slate-800 bg-slate-950/70 px-6 py-5 shadow-sm shadow-black/40 flex flex-col">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-slate-900">
                  <span className="text-lg">🏀</span>
                </div>
                <div>
                  <h2 className="text-sm font-semibold text-zinc-100">
                    Sample teams
                  </h2>
                  <p className="text-[11px] text-zinc-500">
                    Sanity-check that /teams is wired correctly.
                  </p>
                </div>
              </div>
              <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                GET /teams
              </span>
            </div>

            {loading ? (
              <p className="text-xs text-zinc-500">Loading…</p>
            ) : teams.length === 0 ? (
              <p className="text-xs text-zinc-500">
                No teams returned from API.
              </p>
            ) : (
              <div className="mt-2 max-h-64 overflow-y-auto pr-1">
                <ul className="space-y-1.5 text-xs text-zinc-300">
                  {teams.map((t) => (
                    <li key={t.team_id}>
                      <div className="flex items-center justify-between gap-2 rounded-lg px-2 py-1.5 hover:bg-zinc-900/70 transition-colors">
                        <span className="truncate">
                          <span className="text-zinc-500 mr-1">
                            #{t.team_id}
                          </span>
                          {t.name}
                        </span>
                        <span className="flex items-center gap-1 text-[10px] text-zinc-500">
                          <span>{sportIconFromId(t.sport_id)}</span>
                          <span className="uppercase tracking-[0.16em]">
                            {sportLabelFromId(t.sport_id)}
                          </span>
                        </span>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          {/* EVENTS CARD */}
          <div className="rounded-2xl border border-slate-800 bg-slate-950/70 px-6 py-5 shadow-sm shadow-black/40 flex flex-col">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-slate-900">
                  <span className="text-lg">📆</span>
                </div>
                <div>
                  <h2 className="text-sm font-semibold text-zinc-100">
                    Sample events
                  </h2>
                  <p className="text-[11px] text-zinc-500">
                    Quick view of schedule rows and statuses.
                  </p>
                </div>
              </div>
              <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                GET /events
              </span>
            </div>

            {loading ? (
              <p className="text-xs text-zinc-500">Loading…</p>
            ) : events.length === 0 ? (
              <p className="text-xs text-zinc-500">
                No events returned from API.
              </p>
            ) : (
              <>
                <div className="mt-2 max-h-64 overflow-y-auto pr-1">
                  <ul className="space-y-1.5 text-xs text-zinc-300">
                    {visibleEvents.map((e) => (
                      <li key={`${e.event_id}-${e.sport_id}-${e.date}`}>
                        <div className="flex flex-col rounded-lg px-2 py-1.5 hover:bg-zinc-900/70 transition-colors border-b border-zinc-900/60 last:border-b-0">
                          <div className="flex justify-between items-center gap-2">
                            <span className="flex items-center gap-2 min-w-0">
                              <span className="flex items-center gap-1 text-[10px] text-zinc-500 whitespace-nowrap">
                                <span>{sportIconFromId(e.sport_id)}</span>
                                <span className="uppercase tracking-[0.16em]">
                                  {sportLabelFromId(e.sport_id)}
                                </span>
                              </span>

                              <span className="truncate max-w-[140px] sm:max-w-[220px] block">
                                {teamLabel(e.away_team_id)} @{" "}
                                {teamLabel(e.home_team_id)}
                              </span>
                            </span>

                            <div className="flex items-center gap-2">
                              <span className="text-[10px] text-zinc-500">
                                {e.status || "scheduled"}
                              </span>
                              <button
                                className="text-[10px] px-2 py-0.5 rounded-full bg-zinc-800 hover:bg-zinc-700 text-zinc-100"
                                onClick={() => handleViewEvent(e.event_id)}
                              >
                                View
                              </button>
                            </div>
                          </div>

                          <a
                            href={`/games/${e.event_id}`}
                            target="_blank"
                            rel="noreferrer"
                            className="mt-1 text-[10px] text-blue-400 hover:text-blue-300 underline-offset-2 hover:underline"
                          >
                            Open fan view
                          </a>

                          <div className="text-[10px] text-zinc-500">
                            {e.date} · {e.venue || "TBD"}
                          </div>
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>

                {events.length > MAX_PREVIEW && (
                  <div className="flex justify-end pt-2">
                    <button
                      type="button"
                      onClick={() => setShowAll((prev) => !prev)}
                      className="text-[11px] text-blue-400 hover:text-blue-300 underline underline-offset-2"
                    >
                      {showAll
                        ? "Collapse to recent games"
                        : `View all ${events.length.toLocaleString()} games`}
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </section>

        {/* ================================ */}
        {/* 4. EVENT DETAIL PANEL           */}
        {/* ================================ */}
        {selectedEventId !== null && (
          <section className="rounded-2xl border border-slate-800 bg-slate-950/70 px-6 py-5 shadow-sm shadow-black/40 space-y-3">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-semibold text-zinc-100">
                Event #{selectedEventId} details
              </h2>
              {loadingEventDetail && (
                <span className="text-[10px] text-zinc-500">Loading…</span>
              )}
            </div>

            {eventDetailError && (
              <p className="text-xs text-red-400">{eventDetailError}</p>
            )}

            {selectedEvent && !loadingEventDetail && (
              <div className="text-xs text-zinc-300 space-y-3">
                <div className="flex flex-wrap gap-4">
                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">
                      Date
                    </div>
                    <div>{selectedEvent.date}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">
                      Sport
                    </div>
                    <div className="flex items-center gap-1">
                      <span>{sportIconFromId(selectedEvent.sport_id)}</span>
                      <span className="uppercase tracking-[0.16em]">
                        {sportLabelFromId(selectedEvent.sport_id)}
                      </span>
                    </div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">
                      Status
                    </div>
                    <div>{selectedEvent.status ?? "scheduled"}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">
                      Venue
                    </div>
                    <div>{selectedEvent.venue ?? "TBD"}</div>
                  </div>
                </div>

                <div className="flex flex-wrap gap-4">
                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">
                      Home team
                    </div>
                    <div>{teamLabel(selectedEvent.home_team_id)}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">
                      Away team
                    </div>
                    <div>{teamLabel(selectedEvent.away_team_id)}</div>
                  </div>
                </div>

                <details className="mt-2">
                  <summary className="cursor-pointer text-[10px] text-zinc-500">
                    Raw event JSON
                  </summary>
                  <pre className="mt-2 bg-zinc-900/80 p-3 rounded-lg text-[11px] overflow-x-auto">
                    {JSON.stringify(selectedEvent, null, 2)}
                  </pre>
                </details>
              </div>
            )}
          </section>
        )}

        {/* ================================ */}
        {/* 5. PREDICT PANEL                 */}
        {/* ================================ */}
        {/* Card wrapper only – PredictPanel logic untouched */}
        <section className="rounded-2xl border border-slate-800 bg-slate-950/70 px-6 py-6 shadow-sm shadow-black/40 space-y-3">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold text-zinc-100">
                Predict win probability
              </h2>
              <p className="text-[11px] text-zinc-500">
                Query your NBA model for any matchup.
              </p>
            </div>
            <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
              POST /predict/nba
            </span>
          </div>

          <div className="mt-1">
            <PredictPanel />
          </div>
        </section>

        {/* ================================ */}
        {/* 6. RECENT PREDICTIONS            */}
        {/* ================================ */}
        {/* Styling wrapper only – PredictionsPanel logic untouched */}
        <section className="rounded-2xl border border-slate-800 bg-slate-950/70 px-6 py-6 shadow-sm shadow-black/40 space-y-3">
          <div className="flex items-center justify-between mb-1">
            <div>
              <h2 className="text-sm font-semibold text-zinc-100">
                Recent predictions
              </h2>
              <p className="text-[11px] text-zinc-500">
                Monitor how the model is behaving across recent games.
              </p>
            </div>
            <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
              GET /predictions
            </span>
          </div>

          <div className="mt-1">
            <PredictionsPanel />
          </div>
        </section>

        {/* ================================ */}
        {/* 7. EXPLAIN A PREDICTION          */}
        {/* ================================ */}
        {/* Wrapper styling + intro only – InsightsPanel logic untouched */}
        <section className="rounded-2xl border border-slate-800 bg-slate-950/70 px-6 py-6 shadow-sm shadow-black/40 space-y-3 mb-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-sky-500/10 border border-sky-500/40">
                <span className="text-sky-400 text-lg">🧠</span>
              </div>
              <div>
                <h2 className="text-sm font-semibold text-zinc-100">
                  Explain a prediction
                </h2>
                <p className="text-[11px] text-zinc-500">
                  Inspect feature-based insights for any game id.
                </p>
              </div>
            </div>
            <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
              GET /insights/{"{game_id}"}
            </span>
          </div>

          <div className="mt-1">
            <InsightsPanel />
          </div>
        </section>
      </div>
    </main>
  );
}