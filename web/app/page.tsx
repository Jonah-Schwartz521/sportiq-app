// Updated file with proper team name handling and bugfixes
"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Event, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";
import { buildTeamsById, teamLabelFromMap } from "@/lib/teams";

type BoardState = {
  loading: boolean;
  error: string | null;
  events: Event[];
};

function formatDateLabel(dateStr: string | null): string {
  if (!dateStr) return "Today";
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return dateStr;
  return d.toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });
}

function getTodayString(): string {
  return new Date().toISOString().slice(0, 10);
}

// Small badge pill used in multiple places
function Badge({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "neutral" | "accent";
}) {
  const base =
    "inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[10px] font-medium tracking-[0.16em] uppercase";
  const neutral =
    "border-zinc-700/80 bg-zinc-950/80 text-zinc-300 shadow-sm shadow-black/40";
  const accent =
    "border-blue-500/80 bg-blue-500/10 text-blue-100 shadow-sm shadow-blue-500/40";
  return (
    <span className={`${base} ${tone === "accent" ? accent : neutral}`}>
      {children}
    </span>
  );
}

export default function HomePage() {
  const [board, setBoard] = useState<BoardState>({
    loading: true,
    error: null,
    events: [],
  });

  const [teams, setTeams] = useState<Team[]>([]);

  const teamsById = useMemo(() => buildTeamsById(teams), [teams]);

  useEffect(() => {
    (async () => {
      try {
        // Fetch events + teams in parallel
        const [eventsRes, teamsRes] = await Promise.all([
          api.events(),
          api.teams(),
        ]);

        const all = eventsRes.items || [];
        const todayStr = getTodayString();
        const todays = all.filter((e) => e.date === todayStr);
        const fallback = todays.length > 0 ? todays : all;

        setBoard({
          loading: false,
          error: null,
          events: fallback.slice(0, 4),
        });

        setTeams(teamsRes.items || []);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load today’s board";
        setBoard({
          loading: false,
          error: message,
          events: [],
        });
      }
    })();
  }, []);

  const primarySportId = board.events[0]?.sport_id ?? 1;
  const sportLabel =
    board.events.length === 0 ? "All sports" : sportLabelFromId(primarySportId);
  const gamesCount = board.events.length;
  const dateLabel =
    board.events.length === 0 ? "Today" : formatDateLabel(board.events[0]?.date ?? null);

  return (
    <main className="min-h-screen bg-black px-4 pb-16 pt-8 text-white">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-12">
        {/* ========================= */}
        {/* HERO SECTION              */}
        {/* ========================= */}
        <section className="relative flex flex-col gap-8 md:flex-row md:items-center md:justify-between">
          {/* Background spotlight / gradient */}
          <div className="pointer-events-none absolute inset-x-0 top-0 -z-10 mx-auto h-80 max-w-4xl rounded-[40px] bg-[radial-gradient(circle_at_top,_rgba(56,189,248,0.16)_0,_transparent_60%)] blur-3xl" />

          {/* Left: copy + CTAs */}
          <div className="max-w-xl space-y-5">
            <Badge tone="accent">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
              <span>Multi-sport model prototype</span>
            </Badge>

            <div className="space-y-3">
              <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl lg:text-5xl">
                AI-powered{" "}
                <span className="bg-gradient-to-r from-sky-400 via-blue-400 to-indigo-400 bg-clip-text text-transparent">
                  sports insights
                </span>{" "}
                for fans, bettors, and data nerds.
              </h1>
              <p className="max-w-lg text-sm leading-relaxed text-zinc-300 sm:text-base">
                SportIQ runs a unified modeling engine across NBA, NFL, MLB, NHL,
                and UFC. Explore probability surfaces, value mismatches, and
                transparent model explanations – all in one clean interface.
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3 pt-1">
              <Link
                href="/games"
                className="inline-flex items-center justify-center rounded-full bg-gradient-to-r from-sky-500 via-blue-500 to-indigo-500 px-5 py-2.5 text-sm font-medium text-white shadow-lg shadow-blue-500/40 transition-transform transition-colors hover:translate-y-[-1px] hover:shadow-blue-500/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/80"
              >
                View games
                <span className="ml-2 text-xs">↗</span>
              </Link>

              <Link
                href="/admin"
                className="inline-flex items-center justify-center rounded-full border border-zinc-700/80 bg-zinc-950/80 px-4 py-2.5 text-sm font-medium text-zinc-200 shadow-sm shadow-black/40 transition-transform transition-colors hover:border-blue-500/70 hover:bg-zinc-900/90 hover:text-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/80"
              >
                Admin surface
              </Link>

              <span className="ml-1 text-[11px] text-zinc-500">
                Live models in dev, no betting or account required.
              </span>
            </div>
          </div>

          {/* Right: hero model card */}
          <div className="relative w-full max-w-md md:w-96">
            <div className="pointer-events-none absolute -inset-6 -z-10 rounded-[32px] bg-gradient-to-br from-blue-500/25 via-sky-400/10 to-transparent blur-2xl opacity-70" />

            <div className="group rounded-[28px] border border-sky-500/30 bg-gradient-to-br from-zinc-950/95 via-slate-950/95 to-black/95 px-5 py-5 shadow-xl shadow-blue-950/70 backdrop-blur-sm transition-transform hover:-translate-y-1 hover:shadow-blue-500/40">
              <div className="mb-3 flex items-center justify-between">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.14em] text-zinc-500">
                    Live probability surface
                  </p>
                  <p className="text-sm font-medium text-zinc-100">
                    Tonight&apos;s NBA edge snapshot
                  </p>
                </div>
                <Badge tone="neutral">
                  <span className="text-xs">🤖</span>
                  <span>Model v1.2</span>
                </Badge>
              </div>

              {/* Fake match-up preview */}
              <div className="mt-3 space-y-3 rounded-2xl border border-zinc-800 bg-zinc-950/70 p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="space-y-1">
                    <p className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
                      Featured edge
                    </p>
                    <p className="text-sm font-semibold text-zinc-100">
                      Celtics @ Heat
                    </p>
                    <p className="text-[11px] text-zinc-400">Dec 4 • 7:30 PM ET</p>
                  </div>
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/70 bg-emerald-500/10 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] text-emerald-200">
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" />
                    Value flag
                  </span>
                </div>

                {/* Odds bar */}
                <div className="mt-1 space-y-2">
                  <div className="flex items-center justify-between text-[11px] text-zinc-400">
                    <span>Model win probability</span>
                    <span>Home vs Away</span>
                  </div>
                  <div className="relative h-2.5 overflow-hidden rounded-full bg-zinc-900">
                    <div className="absolute inset-0 bg-gradient-to-r from-emerald-500/70 via-blue-500/60 to-sky-400/70" />
                    <div className="absolute inset-y-0 left-[58%] w-[2px] bg-zinc-950/80 opacity-80" />
                  </div>
                  <div className="flex items-center justify-between text-[11px] text-zinc-300">
                    <span className="flex items-center gap-1">
                      🏠 Heat
                      <span className="rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] text-emerald-300">
                        58%
                      </span>
                    </span>
                    <span className="flex items-center gap-1">
                      🧳 Celtics
                      <span className="rounded-full bg-sky-500/15 px-2 py-0.5 text-[10px] text-sky-300">
                        42%
                      </span>
                    </span>
                  </div>
                </div>

                {/* Edge tags */}
                <div className="mt-3 flex flex-wrap gap-2 text-[10px] text-zinc-300">
                  <span className="rounded-full bg-zinc-900/80 px-2 py-1">
                    +4.5 spread vs book
                  </span>
                  <span className="rounded-full bg-zinc-900/80 px-2 py-1">
                    Back-to-back fatigue baked in
                  </span>
                  <span className="rounded-full bg-zinc-900/80 px-2 py-1">
                    Injury-adjusted pace model
                  </span>
                </div>
              </div>

              <div className="mt-4 flex items-center justify-between text-[11px] text-zinc-500">
                <span>Transparent, explainable modeling – no black boxes.</span>
                <span className="rounded-full bg-zinc-900/80 px-2 py-1 text-[10px] text-zinc-300">
                  ℹ Model docs
                </span>
              </div>
            </div>
          </div>
        </section>

        {/* ========================= */}
        {/* FEATURE CARDS             */}
        {/* ========================= */}
        <section className="space-y-4">
          <div className="flex items-center justify-between gap-2">
            <div>
              <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-zinc-400">
                Why SportIQ
              </h2>
              <p className="mt-1 text-sm text-zinc-300">
                Built for people who care about edges, not noise.
              </p>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            {/* Card 1 */}
            <div className="group rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-4 shadow-sm shadow-black/50 transition-transform transition-shadow hover:-translate-y-1 hover:border-emerald-500/70 hover:shadow-emerald-500/30">
              <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-xl bg-emerald-500/15 text-lg">
                📊
              </div>
              <h3 className="text-sm font-semibold text-zinc-50">
                Accurate, probability-first edges
              </h3>
              <p className="mt-2 text-xs leading-relaxed text-zinc-400">
                Every game surfaces calibrated win probabilities, spread edges,
                and fair odds so you can scan boards intelligently instead of
                chasing vibes.
              </p>
            </div>

            {/* Card 2 */}
            <div className="group rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-4 shadow-sm shadow-black/50 transition-transform transition-shadow hover:-translate-y-1 hover:border-sky-500/70 hover:shadow-sky-500/30">
              <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-xl bg-sky-500/15 text-lg">
                🧠
              </div>
              <h3 className="text-sm font-semibold text-zinc-50">
                Explainable model decisions
              </h3>
              <p className="mt-2 text-xs leading-relaxed text-zinc-400">
                Dig into feature importance, back-to-back adjustments, injury
                weights, and pace factors so you understand <em>why</em> the
                model likes a side.
              </p>
            </div>

            {/* Card 3 */}
            <div className="group rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-4 shadow-sm shadow-black/50 transition-transform transition-shadow hover:-translate-y-1 hover:border-indigo-500/70 hover:shadow-indigo-500/30">
              <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-xl bg-indigo-500/15 text-lg">
                🖥️
              </div>
              <h3 className="text-sm font-semibold text-zinc-50">
                Fan-friendly and admin power views
              </h3>
              <p className="mt-2 text-xs leading-relaxed text-zinc-400">
                Toggle between clean fan cards and deep admin surfaces with raw
                features, diagnostics, and model version controls.
              </p>
            </div>
          </div>
        </section>

        {/* ========================= */}
        {/* TODAY'S BOARD             */}
        {/* ========================= */}
        <section className="space-y-4">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-zinc-400">
                Today&apos;s board
              </h2>
              <p className="text-xs text-zinc-400">
                Live probabilities auto-updated daily from the same engine that
                powers the games view.
              </p>
            </div>
            <Link
              href="/games"
              className="text-xs font-medium text-sky-400 underline-offset-4 hover:text-sky-300 hover:underline"
            >
              View all games →
            </Link>
          </div>

          <div className="rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-4 shadow-sm shadow-black/60">
            {/* Summary bar */}
            <div className="mb-4 flex items-center justify-between text-[11px] text-zinc-400">
              <div className="flex items-center gap-2">
                <span className="inline-flex items-center gap-1 rounded-full bg-zinc-900/80 px-2 py-1 text-[10px] uppercase tracking-[0.16em] text-zinc-300">
                  <span className="text-xs">
                    {sportIconFromId(primarySportId)}
                  </span>
                  <span>{sportLabel}</span>
                </span>
                <span>•</span>
                <span>
                  {gamesCount} game{gamesCount === 1 ? "" : "s"}
                </span>
                <span>•</span>
                <span>{dateLabel}</span>
              </div>

              <span className="rounded-full border border-dashed border-zinc-700 bg-zinc-950/80 px-2 py-0.5 text-[10px] text-zinc-400">
                ⟳ Board refreshes with new data
              </span>
            </div>

            {/* Content */}
            {board.loading && (
              <div className="flex items-center justify-center py-10 text-xs text-zinc-500">
                <span className="mr-2 h-2 w-2 animate-ping rounded-full bg-sky-400" />
                Loading today&apos;s games…
              </div>
            )}

            {!board.loading && board.error && (
              <div className="rounded-xl border border-red-500/50 bg-red-500/10 px-4 py-6 text-xs text-red-200">
                <p className="font-medium">Unable to load the board.</p>
                <p className="mt-1 text-[11px] text-red-200/80">
                  {board.error}. Try refreshing or visiting the full games page.
                </p>
              </div>
            )}

            {!board.loading && !board.error && board.events.length === 0 && (
              <div className="flex flex-col items-center gap-3 rounded-xl border border-dashed border-zinc-700 bg-zinc-950/80 px-4 py-8 text-center text-xs text-zinc-400">
                <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-zinc-900/80 text-2xl">
                  💤
                </div>
                <div>
                  <p className="text-sm font-medium text-zinc-200">
                    No games found for today.
                  </p>
                  <p className="mt-1 text-[11px] text-zinc-400">
                    Try changing the date or switching sports on the full games
                    page.
                  </p>
                </div>
                <Link
                  href="/games"
                  className="mt-2 inline-flex items-center justify-center rounded-full border border-zinc-700 bg-zinc-950/80 px-4 py-1.5 text-[11px] font-medium text-sky-300 transition hover:border-sky-500 hover:bg-zinc-900/90 hover:text-sky-100"
                >
                  Go to games →
                </Link>
              </div>
            )}

            {!board.loading && !board.error && board.events.length > 0 && (
              <div className="space-y-3">
                {board.events.map((e) => {
                  const homeName = teamLabelFromMap(teamsById, e.home_team_id);
                  const awayName = teamLabelFromMap(teamsById, e.away_team_id);

                  return (
                    <Link
                      href={`/games/${e.event_id}`}
                      key={e.event_id}
                      className="group flex items-center justify-between gap-3 rounded-xl border border-zinc-800 bg-zinc-950/80 px-3 py-2.5 text-xs shadow-sm shadow-black/40 transition-transform transition-colors hover:-translate-y-[1px] hover:border-blue-500/70 hover:bg-zinc-950 hover:shadow-blue-500/25"
                    >
                      <div className="flex flex-1 items-center gap-3">
                        <div className="flex h-8 w-8 items-center justify-center rounded-full bg-zinc-900/90 text-sm">
                          {sportIconFromId(e.sport_id)}
                        </div>
                        <div className="min-w-0">
                          <p className="truncate text-[12px] font-medium text-zinc-100">
                            {awayName}{" "}
                            <span className="text-zinc-500">@</span>{" "}
                            {homeName}
                          </p>
                          <p className="mt-0.5 text-[11px] text-zinc-400">
                            {formatDateLabel(e.date)} • Model edge preview
                          </p>
                        </div>
                      </div>
                      <div className="flex items-center gap-2 text-[11px] text-zinc-300">
                        <span className="rounded-full bg-zinc-900/90 px-2 py-1">
                          View game
                        </span>
                        <span className="text-xs text-sky-400">↗</span>
                      </div>
                    </Link>
                  );
                })}
              </div>
            )}
          </div>
        </section>

        {/* ========================= */}
        {/* LATEST SPORTS BUZZ        */}
        {/* ========================= */}
        <section className="space-y-4">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-zinc-400">
                Latest sports buzz
              </h2>
              <p className="text-xs text-zinc-400">
                Internal model stories, surface checks, and upcoming edges worth
                watching.
              </p>
            </div>
            <span className="rounded-full bg-zinc-950/80 px-3 py-1 text-[10px] text-zinc-400">
              Updated hourly
            </span>
          </div>

          <div className="space-y-2 rounded-2xl border border-zinc-800 bg-gradient-to-b from-zinc-950/95 to-black/95 p-3 shadow-sm shadow-black/60">
            {[
              {
                id: 1,
                league: "NBA",
                icon: "🏀",
                label: "Model story",
                title: "West coast back-to-back penalty strengthened for March slate",
                meta: "Fatigue factor now scales with travel + altitude",
                chip: "This week",
              },
              {
                id: 2,
                league: "NFL",
                icon: "🏈",
                label: "Line discrepancy",
                title: "Model vs book spread gap hits 5+ points on 2 late-season games",
                meta: "Cross-validating totals before surfacing as &quot;strong edge&quot;",
                chip: "In review",
              },
              {
                id: 3,
                league: "UFC",
                icon: "🥊",
                label: "Fight insights",
                title: "Finishing probability model updated for late-round cardio",
                meta: "Strike tempo + wrestling attempts now weighted higher",
                chip: "Last event",
              },
            ].map((item) => (
              <div
                key={item.id}
                className="group flex items-center justify-between gap-3 rounded-xl px-3 py-2.5 transition-colors hover:bg-zinc-900/70"
              >
                <div className="flex flex-1 items-center gap-3">
                  <div className="flex h-9 w-9 items-center justify-center rounded-full bg-zinc-900/90 text-lg">
                    {item.icon}
                  </div>
                  <div className="min-w-0 space-y-0.5">
                    <div className="flex flex-wrap items-center gap-1 text-[11px]">
                      <span className="rounded-full bg-zinc-900/90 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] text-zinc-300">
                        {item.league}
                      </span>
                      <span className="text-[10px] text-zinc-500">•</span>
                      <span className="text-[10px] text-sky-400">
                        {item.label}
                      </span>
                    </div>
                    <p className="truncate text-[13px] font-medium text-zinc-50">
                      {item.title}
                    </p>
                    <p className="truncate text-[11px] text-zinc-400">
                      {item.meta}
                    </p>
                  </div>
                </div>
                <div className="flex flex-col items-end gap-1 text-[10px] text-zinc-400">
                  <span className="rounded-full bg-zinc-900/90 px-2 py-0.5 text-[10px] text-zinc-300">
                    {item.chip}
                  </span>
                  <span className="text-xs text-sky-400 opacity-0 transition-opacity group-hover:opacity-100">
                    Open board ↗
                  </span>
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </main>
  );
}