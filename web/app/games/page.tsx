"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";

// 🔹 1) Sport label helper (you already had this)
function sportLabelFromId(id: number | null): string {
  switch (id) {
    case 1:
      return "NBA";
    case 2:
      return "MLB";
    case 3:
      return "NFL";
    case 4:
      return "NHL";
    case 5:
      return "UFC";
    default:
      return "Unknown";
  }
}

// 🔹 2) Sport icon helper (new)
function sportIconFromId(id: number | null): string {
  switch (id) {
    case 1:
      return "🏀";
    case 2:
      return "⚾️";
    case 3:
      return "🏈";
    case 4:
      return "🏒";
    case 5:
      return "🥊";
    default:
      return "🏟️";
  }
}

export default function GamesPage() {
  const [events, setEvents] = useState<Event[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // ❌ removed the stray `"🏟️";` here – it did nothing

  // Fetch events + teams
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

  // Build quick lookup for team names
  const teamsById = useMemo(() => {
    const map = new Map<number, Team>();
    for (const t of teams) {
      map.set(t.team_id, t);
    }
    return map;
  }, [teams]);

  function teamLabel(id: number | null): string {
    if (id == null) return "TBD";
    const team = teamsById.get(id);
    if (!team) return `#${id}`;
    return team.name;
  }

  return (
    <main className="min-h-screen bg-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-4xl space-y-6">
        {/* Header */}
        <header className="flex items-baseline justify-between gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">Games</h1>
          <p className="text-xs text-zinc-500">
            Fan view powered by the same API as{" "}
            <span className="font-mono">/admin</span>.
          </p>
        </header>

        {/* Status */}
        {loading && <p className="text-sm text-zinc-500">Loading games…</p>}

        {error && <p className="text-sm text-red-400">{error}</p>}

        {!loading && !error && events.length === 0 && (
          <p className="text-sm text-zinc-500">No games available.</p>
        )}

        {/* Cards */}
        <div className="grid gap-3 sm:grid-cols-2">
          {events.map((e) => (
            <Link
              key={e.event_id}
              href={`/games/${e.event_id}`}
              className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-4 py-3 flex flex-col gap-1 hover:border-zinc-600 transition-colors"
            >
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-zinc-100">
                  {teamLabel(e.home_team_id)} vs {teamLabel(e.away_team_id)}
                </span>

                {/* 🔹 3) Sport icon + label go here */}
                <span className="flex items-center gap-1 text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                  <span>{sportIconFromId(e.sport_id)}</span>
                  <span>{sportLabelFromId(e.sport_id)}</span>
                </span>
              </div>

              <div className="text-[11px] text-zinc-400">
                {e.date} · {e.venue || "TBD"}
              </div>

              <div className="text-[11px] text-zinc-400">
                Status: {e.status || "scheduled"}
              </div>

              <div className="mt-2 text-[11px] text-blue-400">
                View win probability →
              </div>
            </Link>
          ))}
        </div>
      </div>
    </main>
  );
}