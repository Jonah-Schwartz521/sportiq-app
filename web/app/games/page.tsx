"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";
import { TeamValueBadge } from "@/lib/logos";

type SportFilterId = "all" | 1 | 2 | 3 | 4 | 5;

// Format American odds for display (e.g. 182 -> "+182", -140 -> "-140")
function formatAmericanOdds(value: number | null | undefined): string | null {
  if (value === null || value === undefined || Number.isNaN(value)) return null;
  return value > 0 ? `+${value}` : `${value}`;
}

// --- Date / year helpers ---
function getYearFromDate(dateStr: string | null | undefined): string | null {
  if (!dateStr) return null;
  // assuming "YYYY-MM-DD" shape
  return dateStr.slice(0, 4);
}

// UI-only status helper, so we can style FINAL / LIVE / SCHEDULED
function getEventStatus(e: Event, isFinal: boolean): "final" | "scheduled" | "live" {
  if (e.status === "in_progress" || e.status === "live") return "live";
  if (isFinal || e.status === "final") return "final";
  return "scheduled";
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

  const [showScrollTop, setShowScrollTop] = useState(false);

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

  const scrollToTop = useCallback(() => {
    if (typeof window === "undefined") return;
    window.scrollTo({ top: 0, behavior: "smooth" });
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const onScroll = () => {
      // Show button after user has scrolled down a bit
      setShowScrollTop(window.scrollY > 400);
    };

    window.addEventListener("scroll", onScroll);
    onScroll();

    return () => {
      window.removeEventListener("scroll", onScroll);
    };
  }, []);

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

  const visibleCount = visibleEvents.length;

  return (
    <main className="min-h-screen bg-black px-4 pb-10 pt-8 text-white">
      <div className="mx-auto w-full max-w-6xl space-y-6">
        {/* ========================= */}
        {/* 1. PAGE HEADER            */}
        {/* ========================= */}
        <section className="rounded-2xl border border-slate-900 bg-gradient-to-b from-slate-950/90 to-black/90 px-5 py-5 shadow-sm shadow-black/40">
          <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">
                Games
              </h1>
              <p className="mt-1 max-w-xl text-xs text-zinc-400 sm:text-sm">
                Explore today&apos;s lines and historical results. Fan view
                powered by the same API as{" "}
                <span className="font-mono text-zinc-300">/admin</span>.
              </p>
            </div>
          </header>
        </section>

        {/* ========================= */}
        {/* 2. FILTER BAR             */}
        {/* ========================= */}
        <section className="space-y-3 rounded-2xl border border-zinc-900/80 bg-black/90 px-4 py-3">
          <div className="space-y-3">
            {/* Main filter row */}
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              {/* Left: Season + Team */}
              <div className="flex flex-wrap items-center gap-4 text-xs">
                {/* Season selector */}
                <div className="flex items-center gap-2">
                  <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                    Season
                  </span>
                  <div className="relative inline-flex items-center">
                    <select
                      value={yearFilter}
                      onChange={(e) => setYearFilter(e.target.value)}
                      className="rounded-full border border-zinc-700/80 bg-zinc-950 px-3 py-1.5 pr-7 text-xs text-zinc-100 shadow-sm shadow-black/40 transition focus:outline-none focus-visible:border-blue-500/80 focus-visible:ring-2 focus-visible:ring-blue-500/70"
                    >
                      <option value="all">All years</option>
                      {yearOptions.map((y) => (
                        <option key={y} value={y}>
                          {y}
                        </option>
                      ))}
                    </select>
                    <span className="pointer-events-none absolute right-2 text-[10px] text-zinc-500">
                      ⌄
                    </span>
                  </div>
                </div>

                {/* Team selector */}
                <div className="flex items-center gap-2">
                  <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                    Team
                  </span>
                  <div className="relative inline-flex items-center">
                    <select
                      value={teamFilter}
                      onChange={(e) => setTeamFilter(e.target.value)}
                      className="rounded-full border border-zinc-700/80 bg-zinc-950 px-3 py-1.5 pr-7 text-xs text-zinc-100 shadow-sm shadow-black/40 transition focus:outline-none focus-visible:border-blue-500/80 focus-visible:ring-2 focus-visible:ring-blue-500/70"
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
              </div>

              {/* Middle: Date selector + Jump to today */}
              <div className="flex flex-wrap items-center gap-2 text-xs">
                <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                  Date
                </span>
                <div className="flex items-center gap-2">
                  {/* Previous day */}
                  <button
                    type="button"
                    onClick={() => handleShiftDate(-1)}
                    className="flex h-8 w-8 items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/80 text-xs text-zinc-300 shadow-sm shadow-black/30 transition hover:border-blue-500 hover:text-blue-200"
                    aria-label="Previous day"
                  >
                    ←
                  </button>
                  {/* Native date picker as a pill */}
                  <input
                    type="date"
                    value={dateFilter ?? ""}
                    onChange={(e) =>
                      setDateFilter(
                        e.target.value === "" ? null : e.target.value,
                      )
                    }
                    className="min-w-[160px] rounded-full border border-zinc-700/80 bg-zinc-950 px-3 py-1.5 text-xs text-zinc-100 shadow-sm shadow-black/40 transition focus:outline-none focus-visible:border-blue-500/80 focus-visible:ring-2 focus-visible:ring-blue-500/70"
                  />
                  {/* Next day */}
                  <button
                    type="button"
                    onClick={() => handleShiftDate(1)}
                    className="flex h-8 w-8 items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/80 text-xs text-zinc-300 shadow-sm shadow-black/30 transition hover:border-blue-500 hover:text-blue-200"
                    aria-label="Next day"
                  >
                    →
                  </button>
                  {/* Jump to today */}
                  <button
                    type="button"
                    onClick={handleJumpToToday}
                    className="rounded-full border border-zinc-700 bg-zinc-900/80 px-3 py-1 text-[11px] text-zinc-300 shadow-sm shadow-black/30 transition hover:border-blue-500 hover:bg-zinc-900 hover:text-blue-200"
                  >
                    Jump to today
                  </button>
                </div>
              </div>

              {/* Right: Sport chips */}
              <div className="flex w-full items-center gap-2 sm:w-auto">
                <span className="hidden text-[10px] uppercase tracking-[0.16em] text-zinc-500 sm:inline">
                  Sport
                </span>
                <div className="flex gap-2 overflow-x-auto py-1 text-[11px] scrollbar-thin scrollbar-track-transparent scrollbar-thumb-zinc-800">
                  {sportFilters.map((f) => (
                    <button
                      key={f.id}
                      onClick={() => setSelectedSport(f.id)}
                      className={
                        "shrink-0 rounded-full border px-3 py-1.5 shadow-sm transition " +
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

            {/* Daily insights strip */}
            <div className="flex items-center justify-between rounded-xl border border-zinc-800 bg-zinc-950/80 px-3 py-2 text-[11px] text-zinc-300">
              <span>
                Showing{" "}
                <span className="font-semibold text-zinc-50">
                  {visibleCount}
                </span>{" "}
                game{visibleCount === 1 ? "" : "s"}
                {selectedSport !== "all" && (
                  <>
                    {" "}
                    in{" "}
                    <span className="uppercase tracking-[0.14em]">
                      {
                        sportFilters.find((f) => f.id === selectedSport)
                          ?.label
                      }
                    </span>
                  </>
                )}
                {yearFilter !== "all" && (
                  <>
                    {" "}
                    for{" "}
                    <span className="uppercase tracking-[0.14em]">
                      {yearFilter}
                    </span>
                  </>
                )}
                {dateFilter && (
                  <>
                    {" "}
                    on{" "}
                    <span className="font-mono text-zinc-200">
                      {dateFilter}
                    </span>
                  </>
                )}
              </span>

              {loading && (
                <span className="text-[10px] text-zinc-500">
                  Loading games…
                </span>
              )}
              {error && (
                <span className="text-[10px] text-red-400">
                  {error}
                </span>
              )}
            </div>

            {!loading && !error && visibleEvents.length === 0 && (
              <p className="text-[11px] text-zinc-500">
                {dateFilter
                  ? "No games scheduled for this date."
                  : "No games available for the selected filters."}
              </p>
            )}
          </div>
        </section>

        {/* ========================= */}
        {/* 3. GAME CARDS GRID        */}
        {/* ========================= */}
        <section>
          <div className="grid gap-5 sm:grid-cols-2">
            {visibleEvents.map((e) => {
              // Combine *_score with *_pts so 2025 games show as Final
              const homeScore =
                (e as any).home_score ?? (e as any).home_pts ?? null;
              const awayScore =
                (e as any).away_score ?? (e as any).away_pts ?? null;

              const hasScores =
                homeScore !== null && awayScore !== null;

              // Treat as final if we have both scores
              const isFinal = hasScores;

              // Determine homeWin from field or from scores
              const homeWin =
                e.home_win != null
                  ? Boolean(e.home_win)
                  : hasScores
                  ? homeScore > awayScore
                  : null;

              const homeIsWinner = isFinal && homeWin === true;
              const awayIsWinner = isFinal && homeWin === false;

              // Model odds (for scheduled games)
              const homeAmericanOdds =
                (e as any).model_home_american_odds ?? null;
              const awayAmericanOdds =
                (e as any).model_away_american_odds ?? null;

              const homeValue = isFinal
                ? homeScore !== null
                  ? String(homeScore)
                  : null
                : formatAmericanOdds(homeAmericanOdds);

              const awayValue = isFinal
                ? awayScore !== null
                  ? String(awayScore)
                  : null
                : formatAmericanOdds(awayAmericanOdds);

              const showValueRow =
                isFinal || homeValue !== null || awayValue !== null;

              const status = getEventStatus(e, isFinal);

              const statusClasses =
                status === "final"
                  ? "border-emerald-500/60 bg-emerald-500/10 text-emerald-300"
                  : status === "live"
                  ? "border-red-500/70 bg-red-500/10 text-red-300"
                  : "border-transparent bg-zinc-800 text-zinc-200";

              const statusLabel =
                status === "final"
                  ? "Final"
                  : status === "live"
                  ? "Live"
                  : "Scheduled";

              return (
                <Link
                  key={e.event_id}
                  href={`/games/${e.event_id}`}
                  prefetch={false}
                  className="group relative flex flex-col gap-3 overflow-hidden rounded-2xl border border-zinc-800 bg-gradient-to-br from-zinc-950/95 via-black to-zinc-950/90 px-6 py-5 shadow-sm shadow-black/60 transition-transform transition-colors hover:-translate-y-[2px] hover:border-blue-500/70 hover:shadow-blue-500/25"
                >
                  {/* Vertical accent bar */}
                  <div className="pointer-events-none absolute left-0 top-0 h-full w-1 bg-gradient-to-b from-blue-500 via-sky-500 to-transparent opacity-70" />

                  {/* Top row: matchup + league/status badges */}
                  <div className="flex items-start justify-between gap-3 pl-1">
                    <div className="min-w-0 flex-1">
                      {/* Matchup: main headline */}
                      <h2 className="truncate text-sm font-semibold leading-snug text-zinc-50 sm:text-base">
                        {teamLabel(e.away_team_id)}{" "}
                        <span className="text-zinc-500">@</span>{" "}
                        {teamLabel(e.home_team_id)}
                      </h2>

                      {/* Date row */}
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
                          "inline-flex items-center rounded-full border px-2 py-0.5 text-[9px] uppercase tracking-[0.16em] " +
                          statusClasses
                        }
                      >
                        {status === "live" && (
                          <span className="mr-1 inline-flex h-1.5 w-1.5 animate-pulse rounded-full bg-red-400" />
                        )}
                        {statusLabel}
                      </span>
                    </div>
                  </div>

                  {/* Value row – team badges show score for finals or model odds for scheduled games */}
                  {showValueRow && (
                    <div className="mt-3 space-y-2 pl-1">
                      {/* Away team row */}
                      <div className="flex items-center justify-between gap-3">
                        <div
                          className={
                            "rounded-full border px-0.5 py-0.5 transition " +
                            (awayIsWinner
                              ? "border-emerald-400 bg-emerald-500/20 shadow-sm shadow-emerald-500/40"
                              : "border-zinc-700 bg-zinc-900/60")
                          }
                        >
                          <TeamValueBadge
                            teamName={teamLabel(e.away_team_id)}
                            value={awayValue}
                            variant={isFinal ? "score" : "odds"}
                          />
                        </div>
                        <span className="text-[10px] text-zinc-500">
                          {teamLabel(e.away_team_id)}
                        </span>
                      </div>

                      {/* Home team row */}
                      <div className="flex items-center justify-between gap-3">
                        <div
                          className={
                            "rounded-full border px-0.5 py-0.5 transition " +
                            (homeIsWinner
                              ? "border-emerald-400 bg-emerald-500/20 shadow-sm shadow-emerald-500/40"
                              : "border-zinc-700 bg-zinc-900/60")
                          }
                        >
                          <TeamValueBadge
                            teamName={teamLabel(e.home_team_id)}
                            value={homeValue}
                            variant={isFinal ? "score" : "odds"}
                          />
                        </div>
                        <span className="text-[10px] text-zinc-500">
                          {teamLabel(e.home_team_id)}
                        </span>
                      </div>

                      <div className="flex items-center justify-end gap-2">
                        {isFinal && homeWin !== null && (
                          <span className="inline-flex items-center rounded-full border border-zinc-700 bg-zinc-900/80 px-2 py-0.5 text-[9px] uppercase tracking-[0.16em] text-zinc-200">
                            {homeWin ? "Home win" : "Away win"}
                          </span>
                        )}
                        {!isFinal && (homeValue || awayValue) && (
                          <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
                            Model odds
                          </span>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Footer */}
                  <div className="mt-3 flex items-center justify-between border-t border-zinc-800/70 pt-3 pl-1 text-[11px]">
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
      {showScrollTop && (
        <button
          type="button"
          onClick={scrollToTop}
          className="fixed bottom-6 right-6 inline-flex h-10 w-10 items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/90 text-zinc-200 shadow-lg shadow-black/50 backdrop-blur transition hover:border-blue-500 hover:bg-blue-500/20 hover:text-blue-100"
          aria-label="Back to top"
        >
          ↑
        </button>
      )}
    </main>
  );
}