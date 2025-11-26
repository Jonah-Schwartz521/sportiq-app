"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";

type SportFilterId = "all" | 1 | 2 | 3 | 4 | 5;
const YEARS = [2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015];

// --- Date / year helpers ---
function getYearFromDate(dateStr: string | null | undefined): string | null {
  if (!dateStr) return null;
  // assuming "YYYY-MM-DD" shape
  return dateStr.slice(0, 4);
}

export default function GamesPage() {
  const [events, setEvents] = useState<Event[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSport, setSelectedSport] = useState<SportFilterId>("all");
  const [yearFilter, setYearFilter] = useState<string>("all");
  

// 1) Fetch events + teams
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

// 2) After events load, default to latest year (only once while filter is "all")
useEffect(() => {
  if (events.length === 0) return;
  if (yearFilter !== "all") return;

  const years = Array.from(
    new Set(
      events
        .map((e) => getYearFromDate(e.date))
        .filter((y): y is string => !!y)
    )
  ).sort(); // ["2015","2016",..., "2024"]

  const latest = years[years.length - 1];
  if (latest) {
    setYearFilter(latest);
  }
}, [events, yearFilter]);
  

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

  // Year options derived from events
  const yearOptions = useMemo(() => {
    const years = new Set<string>();
    for (const e of events) {
      const y = getYearFromDate(e.date);
      if (y) years.add(y);
    }
    return Array.from(years).sort();
  }, [events]);

  // Apply sport + year filters
  const visibleEvents = useMemo(() => {
    let filtered = events;

    if (selectedSport !== "all") {
      filtered = filtered.filter((e) => e.sport_id === selectedSport);
    }

    if (yearFilter !== "all") {
      filtered = filtered.filter(
        (e) => getYearFromDate(e.date) === yearFilter,
      );
    }

    return filtered;
  }, [events, selectedSport, yearFilter]);

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
          {yearFilter !== "all" && (
            <>
              {" "}
              in{" "}
              <span className="uppercase tracking-[0.12em]">
                {yearFilter}
              </span>
            </>
          )}
        </p>

        {/* Year + sport filters */}
        <div className="flex flex-wrap items-center justify-between gap-3 mb-2 text-xs">
          {/* Year filter */}
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
              Season
            </span>
            <select
              value={yearFilter}
              onChange={(e) => setYearFilter(e.target.value)}
              className="bg-zinc-950 border border-zinc-800 rounded px-2 py-1 text-xs"
            >
              <option value="all">All years</option>
              {yearOptions.map((y) => (
                <option key={y} value={y}>
                  {y}
                </option>
              ))}
            </select>
          </div>

          {/* Sport filter bar */}
          <div className="flex flex-wrap gap-2 text-xs">
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
              prefetch={false}
              className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-4 py-3 flex flex-col gap-1 space-y-1 hover:border-zinc-600 hover:bg-zinc-900/60 hover:-translate-y-[1px] transition-all"
            >
              <div className="flex items-center justify-between gap-2">
                <span className="text-sm font-medium text-zinc-100 flex-1 min-w-0">
                  <span className="block truncate">
                    {teamLabel(e.home_team_id)} vs {teamLabel(e.away_team_id)}
                  </span>
                </span>

                <span className="flex flex-wrap items-center gap-2 text-[10px] text-zinc-500 uppercase tracking-[0.16em] shrink-0">
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