"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";

type SportFilterId = "all" | 1 | 2 | 3 | 4 | 5;

export default function GamesPage() {
  const [events, setEvents] = useState<Event[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSport, setSelectedSport] = useState<SportFilterId>("all");

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

  // Team lookup using shared helpers
  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);

  function teamLabel(id: number | null): string {
    return teamLabelFromMap(teamsById, id);
  }

  // Sport filter options
  const sportFilters: { id: SportFilterId; label: string }[] = [
    { id: "all", label: "All" },
    { id: 1, label: "NBA" },
    { id: 2, label: "MLB" },
    { id: 3, label: "NFL" },
    { id: 4, label: "NHL" },
    { id: 5, label: "UFC" },
  ];

  // Apply selected sport filter
  const visibleEvents =
    selectedSport === "all"
      ? events
      : events.filter((e) => e.sport_id === selectedSport);

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

        {/* Small count of visible games */}
        <p className="text-[11px] text-zinc-500">
          Showing{" "}
          <span className="text-zinc-200 font-medium">
            {visibleEvents.length}
          </span>{" "}
          game{visibleEvents.length === 1 ? "" : "s"}
          {selectedSport !== "all" && (
            <>
              {" "}
              for{" "}
              <span className="uppercase tracking-[0.12em]">
                {sportFilters.find((f) => f.id === selectedSport)?.label}
              </span>
            </>
          )}
        </p>

        {/* Sport filter bar */}
        <div className="flex flex-wrap gap-2 text-xs mb-2">
          {sportFilters.map((f) => (
            <button
              key={f.id}
              onClick={() => setSelectedSport(f.id)}
              className={
                "px-2 py-1 rounded-full border text-[11px] " +
                (selectedSport === f.id
                  ? "border-blue-500/80 bg-blue-500/10 text-blue-100"
                  : "border-zinc-700 text-zinc-400 hover:border-zinc-500")
              }
            >
              {f.label}
            </button>
          ))}
        </div>

        {/* Status */}
        {loading && <p className="text-sm text-zinc-500">Loading games…</p>}

        {error && <p className="text-sm text-red-400">{error}</p>}

        {!loading && !error && visibleEvents.length === 0 && (
          <p className="text-sm text-zinc-500">No games available.</p>
        )}

        {/* Cards */}
        <div className="grid gap-4 sm:grid-cols-2">
          {visibleEvents.map((e) => (
            <Link
              key={e.event_id}
              href={`/games/${e.event_id}`}
              className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-4 py-3 flex flex-col gap-1 space-y-1 hover:border-zinc-600 hover:bg-zinc-900/60 hover:-translate-y-[1px] transition-all"
            >
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-zinc-100 truncate">
                  {teamLabel(e.home_team_id)} vs {teamLabel(e.away_team_id)}
                </span>

                <span className="flex items-center gap-1 text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                  <span>{sportIconFromId(e.sport_id)}</span>
                  <span>{sportLabelFromId(e.sport_id)}</span>
                </span>
              </div>

              <div className="flex items-center justify-between text-[11px] text-zinc-400">
                <span>
                  {e.date} · {e.venue || "TBD"}
                </span>
                <span className="inline-flex items-center rounded-full border border-zinc-700 px-2 py-0.5 uppercase tracking-[0.16em] text-[9px] text-zinc-400">
                  {e.status || "scheduled"}
                </span>
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