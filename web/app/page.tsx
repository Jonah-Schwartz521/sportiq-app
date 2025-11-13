"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import PredictPanel from "@/components/PredictPanel";
import PredictionsPanel from "@/components/Predictions_Panel";

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
  const [health, setHealth] = useState<string>("checking...");
  const [teams, setTeams] = useState<Team[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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

  return (
    <main className="min-h-screen bg-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-5xl space-y-8">
        {/* Header */}
        <header className="flex flex-col sm:flex-row sm:items-baseline sm:justify-between gap-2">
          <h1 className="text-3xl font-semibold tracking-tight">
            SportIQ Admin Surface
          </h1>
          <p className="text-sm text-zinc-400">
            Backend contracts: /teams, /events, /predict, /predictions,
            /insights
          </p>
        </header>

        {/* Health */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4 flex items-center justify-between">
          <div>
            <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">
              API Health
            </div>
            {error ? (
              <div className="text-red-400 text-sm mt-1">
                {error}
              </div>
            ) : (
              <div className="text-sm mt-1">
                /health →{" "}
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

        {/* Teams + Events */}
        <section className="grid gap-4 md:grid-cols-2">
          {/* Teams */}
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-sm font-medium text-zinc-200">
                Sample Teams
              </h2>
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
                  <li
                    key={t.team_id}
                    className="flex items-center justify-between"
                  >
                    <span className="truncate">
                      #{t.team_id} · {t.name}
                    </span>
                    <span className="text-[10px] text-zinc-500">
                      sport {t.sport_id}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* Events */}
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
              <ul className="space-y-1.5 text-xs text-zinc-300">
                {events.map((e) => (
                  <li
                    key={e.event_id}
                    className="flex flex-col border-b border-zinc-900/60 pb-1 last:border-b-0"
                  >
                    <div className="flex justify-between">
                      <span>
                        Event {e.event_id} · sport {e.sport_id}
                      </span>
                      <span className="text-[10px] text-zinc-500">
                        {e.status || "scheduled"}
                      </span>
                    </div>
                    <div className="text-[10px] text-zinc-500">
                      {e.date} · {e.venue || "TBD"} · home{" "}
                      {e.home_team_id ?? "-"} vs away{" "}
                      {e.away_team_id ?? "-"}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>

        {/* 🔥 Predict Panel */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-6">
          <PredictPanel />
        </section>

        {/* 🔍 Recent Predictions */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-6">
          <PredictionsPanel />
        </section>
      </div>
    </main>
  );
}