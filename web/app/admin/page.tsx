export default function Home() {
  return (
    <main className="min-h-screen bg-black text-white flex items-center justify-center">
      <div className="space-y-3 text-center">
        <h1 className="text-3xl font-semibold tracking-tight">SportIQ</h1>
        <p className="text-sm text-zinc-400">
          Multi-sport win probabilities and model insights.
        </p>
        <p className="text-xs text-zinc-500">
          Admin surface is available at <span className="font-mono">/admin</span>.
        </p>
      </div>
    </main>
  );
}