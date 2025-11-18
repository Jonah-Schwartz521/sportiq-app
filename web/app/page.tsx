import Link from "next/link";

export default function HomePage() {
  return (
    <main className="min-h-[70vh] flex items-center justify-center px-4 py-12">
      <div className="w-full max-w-3xl text-center space-y-8">
        {/* Tiny badge */}
        <div className="inline-flex items-center gap-2 rounded-full border border-zinc-800 bg-zinc-950/70 px-3 py-1 text-[11px] text-zinc-400">
          <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
          <span>Multi-sport model prototype</span>
        </div>

        {/* Big title + copy */}
        <div className="space-y-3">
          <h1 className="text-4xl sm:text-5xl font-semibold tracking-tight text-zinc-50">
            SportIQ
          </h1>
          <p className="text-sm sm:text-base text-zinc-400 max-w-xl mx-auto">
            Multi-sport win probabilities and model insights with a clean fan
            view for games and an admin surface for debugging the API.
          </p>
        </div>

        {/* Path hints */}
        <div className="text-[11px] text-zinc-500 space-y-1">
          <p>
            Admin surface:{" "}
            <span className="font-mono text-zinc-300">/admin</span>
          </p>
          <p>
            Fan games view:{" "}
            <span className="font-mono text-zinc-300">/games</span>
          </p>
        </div>

        {/* Primary actions */}
        <div className="flex flex-wrap items-center justify-center gap-3 pt-2">
          <Link
            href="/games"
            className="rounded-full bg-blue-600 hover:bg-blue-500 px-6 py-2.5 text-sm font-medium text-white transition-colors"
          >
            View Games
          </Link>
          <Link
            href="/admin"
            className="rounded-full border border-zinc-700 px-6 py-2.5 text-sm font-medium text-zinc-100 hover:border-zinc-500 hover:bg-zinc-900 transition-colors"
          >
            Admin Surface
          </Link>
        </div>
      </div>
    </main>
  );
}