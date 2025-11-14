import Link from "next/link";

export default function Home() {
  return (
    <main className="min-h-screen bg-black text-white flex items-center justify-center px-4">
      <div className="space-y-4 text-center">
        <h1 className="text-3xl font-semibold tracking-tight">SportIQ</h1>
        <p className="text-sm text-zinc-400">
          Multi-sport win probabilities and model insights.
        </p>

        <div className="flex flex-col items-center gap-2 text-xs text-zinc-500">
          <p>
            Admin surface is available at{" "}
            <span className="font-mono">/admin</span>.
          </p>
          <p>
            Fan view of games is available at{" "}
            <span className="font-mono">/games</span>.
          </p>
        </div>

        <div className="flex justify-center gap-3 pt-2">
          <Link
            href="/games"
            className="inline-flex items-center rounded-full bg-blue-600 px-4 py-2 text-sm font-medium hover:bg-blue-700"
          >
            View Games
          </Link>

          <Link
            href="/admin"
            className="inline-flex items-center rounded-full border border-zinc-700 px-4 py-2 text-sm text-zinc-200 hover:bg-zinc-900"
          >
            Admin Surface
          </Link>
        </div>
      </div>
    </main>
  );
}