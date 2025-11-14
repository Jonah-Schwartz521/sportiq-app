"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import PredictPanel from "@/components/PredictPanel";
import PredictionsPanel from "@/components/Predictions_Panel";
import InsightsPanel from "@/components/InsightsPanel";

type Team = {
  team_id: number;
  sport_id: number;
  name: string;
};

type Event = {
  event_id: number;
  sport_id: number;
  date: string;
  home_team_id: number | null;
  away_team_id: number | null;
  venue: string | null;
  status: string | null;
};

export default function Home() {
  const [health, setHealth] = useState("checking...");
  const [teams, setTeams] = useState<Team[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);
  const [selectedEventId, setSelectedEventId] = useState<number | null>(null);
  const [loadingEventDetail, setLoadingEventDetail] = useState(false);
  const [eventDetailError, setEventDetailError] = useState<string | null>(null);

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
          err instanceof Error ? err.message : "Failed to load from API";
        setError(message);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

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
          <h1 className="text-3xl font-semibold tracking-tight">SportIQ Admin Surface</h1>
          <p className="text-sm text-zinc-400">
            Backend contracts: /teams, /events, /predict, /predictions, /insights
          </p>
        </header>

        {/* HEALTH */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4 flex items-center justify-between">
          <div>
            <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">API Health</div>

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
        </section>

        {/* TEAMS + EVENTS */}
        <section className="grid gap-4 md:grid-cols-2">

          {/* TEAMS */}
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-sm font-medium text-zinc-200">Sample Teams</h2>
              <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">GET /teams</span>
            </div>

            {loading ? (
              <p className="text-xs text-zinc-500">Loading…</p>
            ) : teams.length === 0 ? (
              <p className="text-xs text-zinc-500">No teams returned from API.</p>
            ) : (
              <ul className="space-y-1.5 text-xs text-zinc-300">
                {teams.map((t) => (
                  <li key={t.team_id} className="flex items-center justify-between">
                    <span className="truncate">#{t.team_id} · {t.name}</span>
                    <span className="text-[10px] text-zinc-500">sport {t.sport_id}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* EVENTS */}
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-sm font-medium text-zinc-200">Sample Events</h2>
              <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">GET /events</span>
            </div>

            {loading ? (
              <p className="text-xs text-zinc-500">Loading…</p>
            ) : events.length === 0 ? (
              <p className="text-xs text-zinc-500">No events returned from API.</p>
            ) : (
              <ul className="space-y-1.5 text-xs text-zinc-300">
                {events.map((e) => (
                  <li key={e.event_id} className="flex flex-col border-b border-zinc-900/60 pb-1 last:border-b-0">
                    <div className="flex justify-between items-center">
                      <span>Event {e.event_id} · sport {e.sport_id}</span>

                      <div className="flex items-center gap-2">
                        <span className="text-[10px] text-zinc-500">{e.status || "scheduled"}</span>
                        <button
                          className="text-[10px] px-2 py-0.5 rounded bg-zinc-800 hover:bg-zinc-700"
                          onClick={() => handleViewEvent(e.event_id)}
                        >
                          View
                        </button>
                      </div>
                    </div>

                    <div className="text-[10px] text-zinc-500">
                      {e.date} · {e.venue || "TBD"} · home {e.home_team_id ?? "-"} vs away {e.away_team_id ?? "-"}
                    </div>
                  </li>
                ))}
              </ul>
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
                    <div className="text-[10px] text-zinc-500 uppercase">Date</div>
                    <div>{selectedEvent.date}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">Sport</div>
                    <div>{selectedEvent.sport_id}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">Status</div>
                    <div>{selectedEvent.status ?? "scheduled"}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">Venue</div>
                    <div>{selectedEvent.venue ?? "TBD"}</div>
                  </div>
                </div>

                <div className="flex flex-wrap gap-4">
                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">Home Team</div>
                    <div>#{selectedEvent.home_team_id ?? "-"}</div>
                  </div>

                  <div>
                    <div className="text-[10px] text-zinc-500 uppercase">Away Team</div>
                    <div>#{selectedEvent.away_team_id ?? "-"}</div>
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

        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-6">
          <InsightsPanel />
        </section>

      </div>
    </main>
  );
}