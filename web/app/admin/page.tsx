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
  // Load model metrics (accuracy, Brier score, etc.)
  useEffect(() => {
    (async () => {
      try {
        const m = await api.metrics();
        setMetrics(m);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error
            ? err.message
            : "Failed to load model metrics";
        setMetricsError(message);
      }
    })();
  }, []);

  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);
  const [selectedEventId, setSelectedEventId] = useState<number | null>(null);
  const [loadingEventDetail, setLoadingEventDetail] = useState(false);
  const [eventDetailError, setEventDetailError] = useState<string | null>(null);

  const[showAll, setShowAll] = useState(false);
  const MAX_PREVIEW = 20;

  // Load health, teams, events
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



  // 🔹 Build team lookup + label helper (shared pattern with /games)
  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);

  function teamLabel(id: number | null): string {
    return teamLabelFromMap(teamsById, id);
  }

  // Click handler for "View"
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
    <main className="min-h-screen bg-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-5xl space-y-8">
        {/* HEADER */}
        <header className="flex flex-col sm:flex-row sm:items-baseline sm:justify-between gap-2">
          <h1 className="text-3xl font-semibold tracking-tight">
            SportIQ Admin Surface
          </h1>
          <p className="text-sm text-zinc-400">
            Backend contracts: /teams, /events, /predict, /predictions,
            /insights
          </p>
        </header>

        {/* HEALTH + MODEL METRICS */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4 space-y-3">
          {/* API health row */}
          <div className="flex items-center justify-between">
            <div>
              <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">
                API Health
              </div>

              {error ? (
                <div className="text-red-400 text-sm mt-1">{error}</div>
              ) : (
                <div className="text-sm mt-1">
                  /health →
                  <span
                    className={
                      health === "ok"
                        ? "text-emerald-400 font-medium ml-1"
                        : "text-yellow-400 ml-1"
                    }
                  >
                    {loading ? "loading..." : health}
                  </span>
                </div>
              )}
            </div>

            <span
              className={
                "h-2.5 w-2.5 rounded-full " +
                (error
                  ? "bg-red-500"
                  : health === "ok"
                  ? "bg-emerald-400"
                  : "bg-zinc-500")
              }
            />
          </div>

          {/* Model metrics row */}
          <div className="border-t border-zinc-800 pt-3">
            <div className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
              Model metrics
            </div>

            {metrics ? (
              <div className="mt-2 grid grid-cols-3 gap-3 text-xs text-zinc-300">
                <div>
                  <div className="text-[10px] text-zinc-500 uppercase">
                    Games evaluated
                  </div>
                  <div className="font-mono">
                    {metrics.num_games.toLocaleString()}
                  </div>
                </div>

                <div>
                  <div className="text-[10px] text-zinc-500 uppercase">
                    Accuracy
                  </div>
                  <div className="font-mono">
                    {(metrics.accuracy * 100).toFixed(1)}%
                  </div>
                </div>

                <div>
                  <div className="text-[10px] text-zinc-500 uppercase">
                    Brier score
                  </div>
                  <div className="font-mono">
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
        </section>

        {/* TEAMS + EVENTS */}
        <section className="grid gap-4 md:grid-cols-2">
          {/* TEAMS */}
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-sm font-medium text-zinc-200">Sample Teams</h2>
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
              <ul className="space-y-1.5 text-xs text-zinc-300">
                {teams.map((t) => (
                  <li key={t.team_id}>
                    <div className="flex items-center justify-between gap-2 rounded-lg px-2 py-1 hover:bg-zinc-900/60 transition-colors">
                      <span className="truncate">
                        #{t.team_id} · {t.name}
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
            )}
          </div>

          {/* EVENTS */}
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-sm font-medium text-zinc-200">
                Sample Events
              </h2>
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
                <ul className="space-y-1.5 text-xs text-zinc-300">
                  {visibleEvents.map((e) => (
                    <li key={e.event_id}>
                      <div className="flex flex-col border-b border-zinc-900/60 pb-1 last:border-b-0 rounded-lg px-2 py-1 hover:bg-zinc-900/60 transition-colors">
                        <div className="flex justify-between items-center">
                          <span className="flex items-center gap-2">
                            <span className="flex items-center gap-1 text-[10px] text-zinc-500">
                              <span>{sportIconFromId(e.sport_id)}</span>
                              <span className="uppercase tracking-[0.16em]">
                                {sportLabelFromId(e.sport_id)}
                              </span>
                            </span>
  
                            <span className="truncate max-w-[120px] sm:max-w-[200px] block">
                              {teamLabel(e.away_team_id)} @ {teamLabel(e.home_team_id)}
                            </span>
                          </span>
  
                          <div className="flex items-center gap-2">
                            <span className="text-[10px] text-zinc-500">
                              {e.status || "scheduled"}
                            </span>
                            <button
                              className="text-[10px] px-2 py-0.5 rounded bg-zinc-800 hover:bg-zinc-700"
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
                          className="text-[10px] text-blue-400 hover:text-blue-300 underline-offset-2 hover:underline"
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

        {/* EVENT DETAIL PANEL */}
        {selectedEventId !== null && (
          <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4 space-y-3">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-medium text-zinc-200">
                Event #{selectedEventId} Details
              </h2>
              {loadingEventDetail && (
                <span className="text-[10px] text-zinc-500">Loading…</span>
              )}
            </div>

            {eventDetailError && (
              <p className="text-xs text-red-400">{eventDetailError}</p>
            )}

            {selectedEvent && !loadingEventDetail && (
              <div className="text-xs text-zinc-300 space-y-2">
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
                      Home Team
                    </div>
                    <div>{teamLabel(selectedEvent.home_team_id)}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">
                      Away Team
                    </div>
                    <div>{teamLabel(selectedEvent.away_team_id)}</div>
                  </div>
                </div>

                <details className="mt-2">
                  <summary className="cursor-pointer text-[10px] text-zinc-500">
                    Raw event JSON
                  </summary>
                  <pre className="mt-1 bg-zinc-900/80 p-3 rounded text-[11px] overflow-x-auto">
                    {JSON.stringify(selectedEvent, null, 2)}
                  </pre>
                </details>
              </div>
            )}
          </section>
        )}

        {/* PREDICT PANEL */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-6">
          <PredictPanel />
        </section>

        {/* RECENT PREDICTIONS */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-6">
          <PredictionsPanel />
        </section>

        {/* INSIGHTS */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-6">
          <InsightsPanel />
        </section>
      </div>
    </main>
  );
}