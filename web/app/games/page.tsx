"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";

type SportFilterId = "all" | 1 | 2 | 3 | 4 | 5;

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

  // Season filter ("Season" dropdown) – default will be set to latest once events load
  const [selectedSport, setSelectedSport] = useState<SportFilterId>("all");
  const [yearFilter, setYearFilter] = useState<string>("all");

  // Team filter – defaults to "All teams"
  const [teamFilter, setTeamFilter] = useState<string>("all");

  // Optional date filter – starts as null ("All dates"), only applied when set
  const [dateFilter, setDateFilter] = useState<string | null>(null);

  // ✅ "Jump to today" helper – sets the calendar date to today and also moves Season to current year
  const handleJumpToToday = () => {
    const today = new Date().toISOString().slice(0, 10);
    setDateFilter(today);
    const year = today.slice(0, 4);
    setYearFilter(year);
  };

  // ✅ Date shift helper – move the current dateFilter backward/forward by N days
  // If no date is selected yet, we treat today as the starting point.
  const handleShiftDate = (deltaDays: number) => {
    const baseDate = dateFilter ? new Date(dateFilter) : new Date();
    if (Number.isNaN(baseDate.getTime())) return;

    baseDate.setDate(baseDate.getDate() + deltaDays);
    const shifted = baseDate.toISOString().slice(0, 10);
    setDateFilter(shifted);
  };

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

  // 2) After events load, default Season filter to latest year ("current season")
  //    This runs only once while yearFilter is still "all".
  useEffect(() => {
    if (events.length === 0) return;
    if (yearFilter !== "all") return;

    const years = Array.from(
      new Set(
        events
          .map((e) => getYearFromDate(e.date))
          .filter((y): y is string => !!y),
      ),
    ).sort();

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

  // Year options derived from events – for the Season dropdown
  const yearOptions = useMemo(() => {
    const years = new Set<string>();
    for (const e of events) {
      const y = getYearFromDate(e.date);
      if (y) years.add(y);
    }
    return Array.from(years).sort();
  }, [events]);

  // Team options derived from teams – for the Team dropdown
  const teamOptions = useMemo(() => {
    const labels = new Set<string>();
    for (const t of teams) {
      if (t.name) {
        labels.add(t.name);
      }
    }
    return Array.from(labels).sort();
  }, [teams]);

  // ================================
  // Composed filtering logic (order)
  // 1) Season (yearFilter)
  // 2) Sport (selectedSport)
  // 3) Team (teamFilter)
  // 4) Optional Date (dateFilter)
  // ================================
  const visibleEvents = useMemo(() => {
    let filtered = events;

    // 1) Season filter – required / primary
    if (yearFilter !== "all") {
      filtered = filtered.filter((e) => getYearFromDate(e.date) === yearFilter);
    }

    // 2) Sport filter
    if (selectedSport !== "all") {
      filtered = filtered.filter((e) => e.sport_id === selectedSport);
    }

    // 3) Team filter
    if (teamFilter !== "all") {
      filtered = filtered.filter((e) => {
        const homeName = teamLabel(e.home_team_id);
        const awayName = teamLabel(e.away_team_id);
        return homeName === teamFilter || awayName === teamFilter;
      });
    }

    // 4) Optional Date filter – only applied if user selected a date
    if (dateFilter) {
      filtered = filtered.filter((e) => e.date === dateFilter);
    }

    return filtered;
  }, [events, yearFilter, selectedSport, teamFilter, dateFilter]);

  return (
    // 🌌 Global page layout: more breathing room + centered max width
    <main className="min-h-screen bg-black text-white px-4 py-10">
      <div className="mx-auto w-full max-w-6xl space-y-6">
        {/* ========================= */}
        {/* 1. PAGE HEADER (Games)    */}
        {/* ========================= */}
        <section className="rounded-2xl border border-slate-900 bg-gradient-to-b from-slate-950/90 to-black/90 px-5 py-5 shadow-sm shadow-black/40">
          {/* Header: stronger hierarchy + better spacing */}
          <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
                Games
              </h1>
              <p className="mt-1 text-xs sm:text-sm text-zinc-400 max-w-xl">
                Fan view powered by the same API as{" "}
                <span className="font-mono text-zinc-300">/admin</span>.
              </p>
            </div>

            {/* Small status line: total visible games */}
            <div className="mt-1 sm:mt-0">
              <p className="text-[11px] text-zinc-500 text-right">
                Showing{" "}
                <span className="text-zinc-100 font-medium">
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
            </div>
          </header>
        </section>

        {/* ========================= */}
        {/* 2. FILTER BAR             */}
        {/* ========================= */}
        <section className="space-y-3">
          {/* Season + team + date + sport filter bar */}
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            {/* Season / Team / Date filters row */}
            <div className="flex flex-wrap items-center gap-4 text-xs">
              {/* Season selector – required season filter */}
              <div className="flex items-center gap-2">
                <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                  Season
                </span>
                <div className="relative inline-flex items-center">
                  <select
                    value={yearFilter}
                    onChange={(e) => setYearFilter(e.target.value)}
                    className="bg-zinc-950 border border-zinc-700/80 rounded-full px-3 py-1.5 pr-7 text-xs text-zinc-100 shadow-sm shadow-black/40 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70 focus-visible:border-blue-500/80 transition"
                  >
                    <option value="all">All years</option>
                    {yearOptions.map((y) => (
                      <option key={y} value={y}>
                        {y}
                      </option>
                    ))}
                  </select>
                  {/* Caret indicator */}
                  <span className="pointer-events-none absolute right-2 text-[10px] text-zinc-500">
                    ⌄
                  </span>
                </div>
              </div>

              {/* Team selector – optional team narrowing */}
              <div className="flex items-center gap-2">
                <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                  Team
                </span>
                <div className="relative inline-flex items-center">
                  <select
                    value={teamFilter}
                    onChange={(e) => setTeamFilter(e.target.value)}
                    className="bg-zinc-950 border border-zinc-700/80 rounded-full px-3 py-1.5 pr-7 text-xs text-zinc-100 shadow-sm shadow-black/40 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70 focus-visible:border-blue-500/80 transition"
                  >
                    <option value="all">All teams</option>
                    {teamOptions.map((name) => (
                      <option key={name} value={name}>
                        {name}
                      </option>
                    ))}
                  </select>
                  <span className="pointer-events-none absolute right-2 text-[10px] text-zinc-500">
                    ⌄
                  </span>
                </div>
              </div>

              {/* ✅ Date filter – browser-native calendar picker + arrows + "Jump to today" */}
              <div className="flex items-center gap-2">
                <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
                  Date
                </span>
                <div className="flex items-center gap-2">
                  {/* Previous day arrow */}
                  <button
                    type="button"
                    onClick={() => handleShiftDate(-1)}
                    className="h-8 w-8 flex items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/80 text-zinc-300 text-xs hover:border-blue-500 hover:text-blue-200 transition shadow-sm shadow-black/30"
                    aria-label="Previous day"
                  >
                    ←
                  </button>
                  {/* input[type="date"] gives you the native calendar popover */}
                  <input
                    type="date"
                    value={dateFilter ?? ""}
                    onChange={(e) =>
                      setDateFilter(
                        e.target.value === "" ? null : e.target.value,
                      )
                    }
                    className="bg-zinc-950 border border-zinc-700/80 rounded-full px-3 py-1.5 text-xs text-zinc-100 shadow-sm shadow-black/40 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70 focus-visible:border-blue-500/80 transition min-w-[160px]"
                  />
                  {/* Next day arrow */}
                  <button
                    type="button"
                    onClick={() => handleShiftDate(1)}
                    className="h-8 w-8 flex items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/80 text-zinc-300 text-xs hover:border-blue-500 hover:text-blue-200 transition shadow-sm shadow-black/30"
                    aria-label="Next day"
                  >
                    →
                  </button>
                  {/* "Jump to today" pill button – triggers handleJumpToToday */}
                  <button
                    type="button"
                    onClick={handleJumpToToday}
                    className="text-[11px] px-3 py-1 rounded-full border border-zinc-700 bg-zinc-900/80 text-zinc-300 hover:border-blue-500 hover:bg-zinc-900 hover:text-blue-200 transition shadow-sm shadow-black/30"
                  >
                    Jump to today
                  </button>
                </div>
              </div>
            </div>

            {/* Sport filter chips – modern toggle style with scroll on mobile */}
            <div className="flex w-full sm:w-auto items-center gap-2">
              <span className="hidden text-[10px] text-zinc-500 uppercase tracking-[0.16em] sm:inline">
                Sport
              </span>
              <div className="flex gap-2 overflow-x-auto scrollbar-thin scrollbar-thumb-zinc-800 scrollbar-track-transparent py-1">
                {sportFilters.map((f) => (
                  <button
                    key={f.id}
                    onClick={() => setSelectedSport(f.id)}
                    className={
                      "shrink-0 px-3 py-1.5 rounded-full text-[11px] border transition shadow-sm " +
                      (selectedSport === f.id
                        ? "border-blue-500/80 bg-blue-500/15 text-blue-50 shadow-blue-500/30"
                        : "border-zinc-700 bg-zinc-950/80 text-zinc-400 hover:border-zinc-500 hover:text-zinc-200")
                    }
                  >
                    {f.label}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Load/error/empty states */}
          {loading && (
            <p className="text-sm text-zinc-500 mt-1">Loading games…</p>
          )}

          {error && (
            <p className="text-sm text-red-400 mt-1">{error}</p>
          )}

          {!loading && !error && visibleEvents.length === 0 && (
            <p className="text-sm text-zinc-500 mt-1">
              {dateFilter
                ? "No games scheduled for this date."
                : "No games available for the selected filters."}
            </p>
          )}
        </section>

        {/* ========================= */}
        {/* 3. GAME CARDS GRID        */}
        {/* ========================= */}
        <section>
          <div className="grid gap-5 sm:grid-cols-2">
            {visibleEvents.map((e) => {
              // ✅ Combine *_score with *_pts so 2025 games show as Final
              const homeScore =
                (e as any).home_score ?? (e as any).home_pts ?? null;
              const awayScore =
                (e as any).away_score ?? (e as any).away_pts ?? null;

              const hasScores =
                homeScore !== null && awayScore !== null;

              // ✅ Treat as final if we have both scores
              const isFinal = hasScores;

              // ✅ Determine homeWin from field or from scores
              const homeWin =
                e.home_win != null
                  ? Boolean(e.home_win)
                  : hasScores
                  ? homeScore > awayScore
                  : null;

              return (
                <Link
                  key={e.event_id}
                  href={`/games/${e.event_id}`}
                  prefetch={false}
                  // Card: sportsbook-style tile with stronger padding, depth, and hover
                  className="group relative overflow-hidden rounded-2xl border border-zinc-800 bg-gradient-to-br from-zinc-950/95 via-black to-zinc-950/90 px-6 py-5 flex flex-col gap-3 shadow-sm shadow-black/60 hover:border-blue-500/70 hover:shadow-blue-500/25 hover:-translate-y-[2px] transition-transform transition-colors"
                >
                  {/* Vertical accent bar */}
                  <div className="pointer-events-none absolute left-0 top-0 h-full w-1 bg-gradient-to-b from-blue-500 via-sky-500 to-transparent opacity-70" />

                  {/* Top row: matchup + league/status badges */}
                  <div className="flex items-start justify-between gap-3 pl-1">
                    <div className="flex-1 min-w-0">
                      {/* Matchup: main headline */}
                      <h2 className="text-sm sm:text-base font-semibold text-zinc-50 leading-snug truncate">
                        {teamLabel(e.away_team_id)}{" "}
                        <span className="text-zinc-500">@</span>{" "}
                        {teamLabel(e.home_team_id)}
                      </h2>

                      {/* Date row – no inline score to avoid duplicates */}
                      <p className="mt-1 text-[11px] text-zinc-400">
                        {e.date}
                      </p>
                    </div>

                    {/* League + status badges */}
                    <div className="flex flex-col items-end gap-1">
                      <span className="inline-flex items-center gap-1 rounded-full border border-zinc-700 bg-zinc-900/80 px-2 py-0.5 text-[9px] uppercase tracking-[0.16em] text-zinc-300">
                        <span className="text-xs">
                          {sportIconFromId(e.sport_id)}
                        </span>
                        <span>{sportLabelFromId(e.sport_id)}</span>
                      </span>
                      <span
                        className={
                          "inline-flex items-center rounded-full border px-2 py-0.5 text-[9px] uppercase tracking-[0.16em]" +
                          (isFinal
                            ? " border-emerald-500/60 bg-emerald-500/10 text-emerald-300"
                            : " border-transparent bg-zinc-800 text-zinc-200")
                        }
                      >
                        {isFinal ? "Final" : "Scheduled"}
                      </span>
                    </div>
                  </div>

                  {/* ✅ Score row – single canonical place for the final score */}
                  {isFinal && (
                    <div className="mt-1 pl-1 flex items-center justify-between gap-2">
                      <span className="text-[11px] text-zinc-200 font-semibold truncate">
                        {teamLabel(e.away_team_id)} {awayScore} @{" "}
                        {teamLabel(e.home_team_id)} {homeScore}
                      </span>
                      <span className="inline-flex items-center rounded-full border border-zinc-700 bg-zinc-900/80 px-2 py-0.5 uppercase tracking-[0.16em] text-[9px] text-zinc-200">
                        {homeWin ? "Home win" : "Away win"}
                      </span>
                    </div>
                  )}

                  {/* Divider before CTA */}
                  <div className="mt-3 border-t border-zinc-800/70 pt-3 pl-1 flex items-center justify-between text-[11px]">
                    <span className="text-zinc-500">
                      Powered by{" "}
                      <span className="font-mono text-zinc-300">
                        nba_logreg_b2b_v1
                      </span>
                    </span>
                    <span className="inline-flex items-center gap-1 text-blue-400 group-hover:text-blue-300">
                      <span className="underline underline-offset-2 group-hover:underline">
                        View box score + model view
                      </span>
                      <span className="text-xs">📈</span>
                    </span>
                  </div>
                </Link>
              );
            })}
          </div>
        </section>
      </div>
    </main>
  );
}