import Link from "next/link";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

// Simple types for the data we show on the landing page
export type TodayGame = {
  gameId: number;
  date: string;
  homeName: string;
  awayName: string;
  pHome: number | null;
  pAway: number | null;
};

export type SportsNewsItem = {
  id: number;
  league: string;
  title: string;
  source: string;
  timestamp: string;
};

// Keep this page dynamic so we can show "today's games" nicely
export const dynamic = "force-dynamic";

async function fetchTodayGames(): Promise<TodayGame[]> {
  const todayIso = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

  try {
    const res = await fetch(`${API_BASE_URL}/events?limit=300`, {
      // Allow Next.js to cache briefly but refresh often
      next: { revalidate: 60 },
    });

    if (!res.ok) {
      console.error("Failed to fetch events", await res.text());
      return [];
    }

    const data = (await res.json()) as { items?: any[] };
    const items = data.items ?? [];

    // Filter to today and cap to a small number of cards
    const todays = items.filter((g) => g.date === todayIso).slice(0, 6);

    if (todays.length === 0) return [];

    // For each game, hit the prediction endpoint to get win probabilities
    const withPreds = await Promise.all(
      todays.map(async (g) => {
        try {
          const predRes = await fetch(
            `${API_BASE_URL}/predict_by_game_id?game_id=${g.event_id}`,
            { next: { revalidate: 60 } }
          );

          if (!predRes.ok) {
            console.error("Prediction failed", await predRes.text());
            return {
              gameId: g.event_id,
              date: g.date,
              homeName: String(g.home_team_id ?? "Home"),
              awayName: String(g.away_team_id ?? "Away"),
              pHome: null,
              pAway: null,
            } as TodayGame;
          }

          const pred = await predRes.json();

          return {
            gameId: pred.game_id,
            date: pred.date,
            homeName: pred.home_team,
            awayName: pred.away_team,
            pHome: pred.p_home,
            pAway: pred.p_away,
          } as TodayGame;
        } catch (err) {
          console.error("Prediction request error", err);
          return {
            gameId: g.event_id,
            date: g.date,
            homeName: String(g.home_team_id ?? "Home"),
            awayName: String(g.away_team_id ?? "Away"),
            pHome: null,
            pAway: null,
          } as TodayGame;
        }
      })
    );

    return withPreds;
  } catch (err) {
    console.error("Failed to fetch today's games", err);
    return [];
  }
}

// For now, this is static mock news – later you can swap to a real feed/API
const mockNews: SportsNewsItem[] = [
  {
    id: 1,
    league: "NBA",
    title: "Young core powers late-season playoff push.",
    source: "SportIQ model notes",
    timestamp: "Updated hourly",
  },
  {
    id: 2,
    league: "NFL",
    title: "QB efficiency reshuffles top-10 power rankings.",
    source: "Internal analytics",
    timestamp: "This week",
  },
  {
    id: 3,
    league: "UFC",
    title: "Underdog finishes surge in late rounds.",
    source: "Fight insights",
    timestamp: "Last event",
  },
];

export default async function HomePage() {
  const todayGames = await fetchTodayGames();

  return (
    // Global layout: keep dark theme, add subtle vertical structure
    <main className="min-h-screen bg-black text-slate-100 flex flex-col">
      <div className="flex-1">
        {/* =========================
            1. HERO SECTION
            ========================= */}
        <section className="relative pt-24 pb-20 sm:pt-28 sm:pb-24">
          {/* Hero background: subtle radial + vertical gradient for premium AI feel */}
          <div
            className="pointer-events-none absolute inset-0 -z-10 bg-[radial-gradient(circle_at_top,_rgba(56,189,248,0.25)_0,_rgba(15,23,42,0.3)_40%,_transparent_75%)]"
            aria-hidden="true"
          />

          <div className="relative mx-auto flex max-w-6xl flex-col gap-10 px-4 lg:flex-row lg:items-center lg:justify-between">
            {/* Left: headline + copy */}
            <div className="max-w-xl text-center lg:text-left">
              <StatusPill />

              {/* Headline hierarchy: bolder + more breathing room */}
              <h1 className="mt-7 text-4xl sm:text-5xl md:text-6xl font-extrabold tracking-tight leading-tight">
                <span className="bg-gradient-to-r from-slate-50 via-slate-100 to-slate-400 bg-clip-text text-transparent">
                  AI-powered sports predictions
                </span>
              </h1>

              <p className="mt-4 text-base sm:text-lg text-slate-400 max-w-xl mx-auto lg:mx-0 leading-relaxed">
                Real-time win probabilities and model insights across NBA and
                more. Built for fans, bettors, and data nerds who want the
                numbers behind the game.
              </p>

              {/* Primary + secondary CTAs with clear hierarchy */}
              <div className="mt-8 flex flex-wrap items-center justify-center gap-3 lg:justify-start">
                <Link
                  href="/games"
                  className="rounded-full bg-blue-500 px-7 py-3 text-sm font-semibold text-white shadow-[0_18px_45px_rgba(59,130,246,0.55)] transition hover:bg-blue-400 hover:shadow-[0_20px_55px_rgba(59,130,246,0.75)]"
                >
                  View games
                </Link>
                <Link
                  href="/admin"
                  className="rounded-full border border-slate-600 bg-slate-950/70 px-7 py-3 text-sm font-medium text-slate-100 transition hover:border-slate-400 hover:bg-slate-900"
                >
                  Admin surface
                </Link>
              </div>

              <p className="mt-4 text-xs text-slate-500">
                Powered by your custom NBA model. Updated daily from live
                scores.
              </p>
            </div>

            {/* Right: refined "model snapshot" card with cleaner glow + spacing */}
            <div className="mx-auto w-full max-w-sm lg:mx-0">
              <div className="relative rounded-3xl border border-white/10 bg-gradient-to-br from-slate-950 via-black to-slate-950 p-5 shadow-[0_28px_70px_rgba(15,23,42,0.9)]">
                {/* subtle overlay grid / glow */}
                <div
                  className="pointer-events-none absolute inset-0 rounded-3xl bg-[radial-gradient(circle_at_top,_rgba(56,189,248,0.22)_0,_transparent_60%),linear-gradient(to_bottom,_rgba(15,23,42,0.5),_rgba(15,23,42,0.95))]"
                  aria-hidden="true"
                />

                <div className="relative space-y-4">
                  {/* Mini win probability card – tighter spacing + clearer type */}
                  <div className="rounded-2xl bg-black/80 p-3 border border-slate-800/80">
                    <div className="flex items-center justify-between text-[11px] text-slate-400">
                      <span>Tonight&apos;s edge</span>
                      <span className="text-sky-300">Model snapshot</span>
                    </div>
                    <div className="mt-2 flex items-center justify-between text-xs text-slate-100">
                      <span className="font-medium">Nuggets</span>
                      <span className="font-semibold text-sky-300">
                        64.5% win
                      </span>
                    </div>
                    <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-slate-900">
                      <div className="h-full w-[64%] rounded-full bg-gradient-to-r from-sky-400 to-emerald-400" />
                    </div>
                    <p className="mt-1.5 text-[10px] text-slate-500">
                      Implied edge:{" "}
                      <span className="text-slate-200">+5–7% vs. 50/50</span>
                    </p>
                  </div>

                  {/* Central IQ chip + orbiting sport icons – cleaned up spacing */}
                  <div className="relative flex items-center justify-center py-4">
                    <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-sky-400/60 bg-slate-950 shadow-[0_0_36px_rgba(56,189,248,0.9)]">
                      <span className="text-[11px] font-semibold tracking-tight text-sky-100">
                        IQ
                      </span>
                    </div>

                    {/* orbiting emojis */}
                    <div className="pointer-events-none absolute inset-0">
                      <div className="absolute -top-1 left-1/2 h-8 w-8 -translate-x-1/2 rounded-full border border-white/10 bg-black/80 text-[11px] text-center leading-8">
                        🏀
                      </div>
                      <div className="absolute left-1 top-1/2 h-8 w-8 -translate-y-1/2 rounded-full border border-white/10 bg-black/80 text-[11px] text-center leading-8">
                        🏈
                      </div>
                      <div className="absolute right-1 top-1/2 h-8 w-8 -translate-y-1/2 rounded-full border border-white/10 bg-black/80 text-[11px] text-center leading-8">
                        🥊
                      </div>
                    </div>
                  </div>

                  {/* Small timeline dots */}
                  <div className="flex items-center justify-between text-[10px] text-slate-500">
                    <span>Pre-game</span>
                    <span>Live</span>
                    <span>Post-game</span>
                  </div>
                  <div className="h-1 w-full rounded-full bg-slate-900">
                    <div className="h-full w-1/3 rounded-full bg-gradient-to-r from-emerald-400 to-sky-400" />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* =========================
            3–5. FEATURES + BOARD + NEWS
            ========================= */}
        <section className="relative border-t border-slate-800/70 bg-gradient-to-b from-black to-slate-950/85">
          <div className="mx-auto max-w-6xl px-4 py-12 space-y-10 md:space-y-12">
            <FeatureRow />

            <div className="grid gap-8 items-start lg:grid-cols-[2fr,1.1fr]">
              <TodayGamesPanel games={todayGames} />
              <NewsPanel items={mockNews} />
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}

function StatusPill() {
  // Small live-status pill with subtle glow
  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/40 bg-emerald-500/10 px-3 py-1 text-xs text-emerald-300 shadow-[0_0_25px_rgba(16,185,129,0.4)]">
      <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
      <span>Multi-sport model prototype</span>
    </div>
  );
}

function FeatureRow() {
  // 3. Feature cards: consistent icons, padding, shadows, hover
  const features = [
    {
      title: "Accurate win probabilities",
      body: "Trained on 11k+ historical NBA games with back-to-backs and rest baked in.",
      iconColor: "text-sky-400",
      icon: (
        <svg
          className="h-5 w-5"
          viewBox="0 0 24 24"
          fill="none"
          aria-hidden="true"
        >
          <path
            d="M4 18.5V5.5M4 18.5H19.5"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
          <path
            d="M7 14L11 10L14 13L19 7"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          <circle cx="7" cy="14" r="0.7" fill="currentColor" />
          <circle cx="11" cy="10" r="0.7" fill="currentColor" />
          <circle cx="14" cy="13" r="0.7" fill="currentColor" />
          <circle cx="19" cy="7" r="0.7" fill="currentColor" />
        </svg>
      ),
    },
    {
      title: "Model explanations",
      body: "See why the model leans a certain way using feature-based insights.",
      iconColor: "text-violet-400",
      icon: (
        <svg
          className="h-5 w-5"
          viewBox="0 0 24 24"
          fill="none"
          aria-hidden="true"
        >
          <circle
            cx="11"
            cy="11"
            r="5"
            stroke="currentColor"
            strokeWidth="1.5"
          />
          <path
            d="M15 15L19 19"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
        </svg>
      ),
    },
    {
      title: "Fan + admin views",
      body: "Clean front-end for fans, plus a debugging surface for you as the builder.",
      iconColor: "text-emerald-400",
      icon: (
        <svg
          className="h-5 w-5"
          viewBox="0 0 24 24"
          fill="none"
          aria-hidden="true"
        >
          <rect
            x="3.75"
            y="5"
            width="16.5"
            height="10"
            rx="1.5"
            stroke="currentColor"
            strokeWidth="1.5"
          />
          <path
            d="M9 19H15"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
          <circle cx="12" cy="10" r="2" fill="currentColor" />
          <path
            d="M8.5 13.75C9.3 12.9 10.3 12.5 12 12.5C13.7 12.5 14.7 12.9 15.5 13.75"
            stroke="currentColor"
            strokeWidth="1.25"
            strokeLinecap="round"
          />
        </svg>
      ),
    },
  ];

  return (
    <div className="grid gap-6 md:grid-cols-3">
      {features.map((f) => (
        <div
          key={f.title}
          className="group rounded-2xl border border-slate-800 bg-slate-950/75 px-5 py-5 text-sm shadow-sm shadow-black/40 transition hover:border-sky-500/50 hover:shadow-[0_20px_40px_rgba(15,23,42,0.95)]"
        >
          <div className="mb-3 inline-flex h-9 w-9 items-center justify-center rounded-full bg-slate-900/80">
            <span className={f.iconColor}>{f.icon}</span>
          </div>
          <h3 className="text-slate-100 font-medium mb-1.5 text-[14px]">
            {f.title}
          </h3>
          <p className="text-slate-400 text-[12px] leading-relaxed">
            {f.body}
          </p>
        </div>
      ))}
    </div>
  );
}

function TodayGamesPanel({ games }: { games: TodayGame[] }) {
  const hasGames = games && games.length > 0;

  // 4. Today's board: feels like an analytics module with structured container
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-950/85 p-5 shadow-sm shadow-black/40">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-sm font-semibold text-slate-100">
            Today&apos;s board
          </h2>
          <p className="text-xs text-slate-500">
            Fresh win probabilities from your model.
          </p>
        </div>
        <Link
          href="/games"
          className="text-[11px] text-slate-400 hover:text-slate-200 underline-offset-2 hover:underline"
        >
          View all games →
        </Link>
      </div>

      {!hasGames && (
        // Improved empty state: icon + dotted border + better spacing
        <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-slate-700 bg-black/70 px-5 py-7 text-center">
          <div className="mb-3 text-2xl">📊</div>
          <p className="text-xs text-slate-300">
            No games on today&apos;s slate yet.
          </p>
          <p className="mt-1.5 text-[11px] text-slate-500 max-w-xs">
            As soon as there are games on the calendar, this module will
            auto-populate from your API.
          </p>
        </div>
      )}

      {hasGames && (
        <ul className="space-y-3">
          {games.map((g) => {
            const pHome = g.pHome ?? 0;
            const pAway = g.pAway ?? 0;
            const total = pHome + pAway || 1;
            const homePct = (pHome / total) * 100;
            const awayPct = (pAway / total) * 100;

            return (
              <li
                key={g.gameId}
                className="rounded-xl border border-slate-800/80 bg-black/75 px-3 py-2.5 text-xs transition hover:border-sky-500/50 hover:bg-black/85"
              >
                <Link href={`/games/${g.gameId}`} className="block">
                  <div className="flex items-center justify-between mb-1.5">
                    <div className="text-slate-200">
                      <span className="font-medium">{g.awayName}</span>
                      <span className="text-slate-500"> @ </span>
                      <span className="font-medium">{g.homeName}</span>
                    </div>
                    <div className="text-[11px] text-slate-400">
                      View model →
                    </div>
                  </div>

                  <div className="flex gap-2 items-center">
                    {/* Win probability bar */}
                    <div className="flex-1 h-1.5 rounded-full bg-slate-900 overflow-hidden">
                      <div
                        className="h-full bg-gradient-to-r from-sky-500 to-emerald-400"
                        style={{ width: `${homePct}%` }}
                      />
                    </div>
                    <div className="flex flex-col items-end text-[11px] text-slate-400 leading-tight">
                      <span>Home {Math.round(pHome * 100)}%</span>
                      <span className="text-slate-600">
                        Away {Math.round(pAway * 100)}%
                      </span>
                    </div>
                  </div>
                </Link>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

function NewsPanel({ items }: { items: SportsNewsItem[] }) {
  // Simple helper just for presentational league labels (still just styling)
  const leagueLabel = (league: string) => {
    if (league === "NBA") return "NBA • Hoops";
    if (league === "NFL") return "NFL • Gridiron";
    if (league === "UFC") return "UFC • Fight night";
    return league;
  };

  // 5. Latest sports buzz: clean news widget with clearer hierarchy
  return (
    <aside className="rounded-2xl border border-slate-800 bg-slate-950/85 p-5 shadow-sm shadow-black/40">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-sm font-semibold text-slate-100">
          Latest sports buzz
        </h2>
        <span className="rounded-full bg-slate-900 px-2.5 py-0.5 text-[10px] text-slate-400">
          Model stories
        </span>
      </div>
      <ul className="space-y-3 text-xs">
        {items.map((item) => (
          <li
            key={item.id}
            className="rounded-xl border border-slate-800/70 bg-black/75 px-3 py-2.5"
          >
            <div className="mb-1.5 flex items-center justify-between">
              <div className="flex items-center gap-1.5">
                {/* Sport label with tiny icon-style feel */}
                <span className="text-[10px] uppercase tracking-wide text-slate-400">
                  {item.league}
                </span>
                <span className="text-[9px] text-slate-500">
                  {leagueLabel(item.league)}
                </span>
              </div>
              <span className="rounded-full bg-slate-900 px-2 py-0.5 text-[10px] text-slate-400">
                {item.timestamp}
              </span>
            </div>
            <p className="text-slate-100 mb-0.5 text-[13px] leading-snug">
              {item.title}
            </p>
            <p className="text-[11px] text-slate-500">{item.source}</p>
          </li>
        ))}
      </ul>
    </aside>
  );
}