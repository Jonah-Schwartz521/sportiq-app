"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  api,
  type PredictionLogItem,
  type PredictionLogResponse,
} from "@/lib/api";
import { formatIsoToLocal, timeAgo } from "@/lib/time";

// Edge filter buckets for the pills
type EdgeFilter = "all" | "coinflip" | "lean" | "strong";

function classifyEdge(pHome: number, pAway: number): EdgeFilter {
  const edge = Math.abs(pHome - pAway);
  if (edge < 0.05) return "coinflip"; // near coin flip
  if (edge < 0.15) return "lean"; // modest edge
  return "strong"; // strong favorite
}

function impliedOdds(prob: number): string {
  if (!prob || prob <= 0) return "-";
  return `${(1 / prob).toFixed(2)}x`;
}

export default function PredictionsPanel() {
  const [data, setData] = useState<PredictionLogItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [filter, setFilter] = useState<EdgeFilter>("all");
  const [filterText, setFilterText] = useState(""); // text search

  // Load recent predictions from FastAPI /predictions (logic unchanged)
  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);

        const res: PredictionLogResponse = await api.predictions(50);
        setData(res.items || []);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load predictions";
        setError(message);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  // Apply edge filter + text search (logic unchanged)
  const filtered = useMemo(() => {
    const text = filterText.trim().toLowerCase();

    return data.filter((i) => {
      // edge bucket filter
      const edgeOk =
        filter === "all" || classifyEdge(i.p_home, i.p_away) === filter;

      // text search filter
      const home = (i.home_team ?? "").toLowerCase();
      const away = (i.away_team ?? "").toLowerCase();

      const textOk =
        text.length === 0 || home.includes(text) || away.includes(text);

      return edgeOk && textOk;
    });
  }, [data, filter, filterText]);

  return (
    // Card wrapper for the whole predictions panel – upgraded to a premium dashboard card
    <div className="rounded-2xl border border-zinc-800 bg-zinc-950/70 p-4 sm:p-5 shadow-sm shadow-black/40 space-y-4">
      {/* Header + filters */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        {/* Title + description */}
        <div>
          <h2 className="text-sm font-semibold text-zinc-100">
            Recent Predictions
          </h2>
          <p className="text-[11px] text-zinc-500">
            Latest calls from the NBA model (logged from{" "}
            <span className="font-mono text-zinc-300 text-[10px]">
              /predict_by_game_id
            </span>
            ).
          </p>
        </div>

        {/* Filters block – text search + edge chips, aligned and sized consistently */}
        <div className="flex flex-col items-stretch gap-3 sm:flex-row sm:items-end">
          {/* Text search with label */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
              Filter by team
            </span>
            <input
              type="text"
              placeholder="Search team name…"
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              className="h-8 min-w-[180px] rounded-full border border-zinc-800 bg-zinc-950 px-3 text-[11px] text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-blue-500/70 focus:border-blue-500/70 transition"
            />
          </div>

          {/* Edge filter pills with label */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] uppercase tracking-[0.16em] text-zinc-500">
              Edge bucket
            </span>
            <div className="flex items-center gap-1 rounded-full bg-zinc-950/80 px-1 py-1 border border-zinc-800">
              {[
                { key: "all", label: "All" },
                { key: "coinflip", label: "Coin flips" },
                { key: "lean", label: "Leans" },
                { key: "strong", label: "Strong edges" },
              ].map((f) => (
                <button
                  key={f.key}
                  type="button"
                  onClick={() => setFilter(f.key as EdgeFilter)}
                  className={
                    "px-2.5 py-1 rounded-full text-[10px] uppercase tracking-[0.14em] transition " +
                    (filter === f.key
                      ? "bg-zinc-100 text-black shadow-sm shadow-black/40"
                      : "bg-transparent text-zinc-400 hover:bg-zinc-900 hover:text-zinc-100")
                  }
                >
                  {f.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Content states */}
      {loading && (
        <p className="text-xs text-zinc-500">Loading predictions…</p>
      )}

      {error && !loading && (
        <p className="text-xs text-red-400">{error}</p>
      )}

      {!loading && !error && filtered.length === 0 && (
        <p className="text-xs text-zinc-500">
          No predictions logged yet. Use the Predict panel or fan view to call
          the model.
        </p>
      )}

      {/* Table */}
      {filtered.length > 0 && (
        <div className="overflow-x-auto rounded-xl border border-zinc-800 bg-zinc-950/80">
          <table className="w-full text-xs text-zinc-300">
            <thead className="text-[10px] uppercase tracking-[0.16em] text-zinc-500 bg-zinc-950 border-b border-zinc-800/80">
              <tr>
                <th className="px-3 py-2 text-left font-medium">Matchup</th>
                <th className="px-3 py-2 text-left font-medium">Probs</th>
                <th className="px-3 py-2 text-left font-medium">Edge</th>
                <th className="px-3 py-2 text-left font-medium">
                  Odds (home/away)
                </th>
                <th className="px-3 py-2 text-left font-medium">Logged</th>
                <th className="px-3 py-2 text-right font-medium">Fan view</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((item, idx) => {
                const edge = Math.abs(item.p_home - item.p_away);

                return (
                  <tr
                    key={`${item.game_id}-${item.created_at}`}
                    className={
                      "border-t border-zinc-900/80 " +
                      (idx % 2 === 0 ? "bg-zinc-950/40" : "bg-transparent")
                    }
                  >
                    {/* Matchup cell – clearer hierarchy */}
                    <td className="px-3 py-2 align-top">
                      <div className="flex flex-col gap-0.5">
                        <span className="font-medium text-zinc-50">
                          {item.away_team} @ {item.home_team}
                        </span>
                        <span className="text-[10px] text-zinc-500">
                          Game ID {item.game_id} · {item.date}
                        </span>
                      </div>
                    </td>

                    {/* Probs */}
                    <td className="px-3 py-2 align-top">
                      <div className="flex flex-col gap-0.5">
                        <span>
                          Home: {(item.p_home * 100).toFixed(1)}%
                        </span>
                        <span>
                          Away: {(item.p_away * 100).toFixed(1)}%
                        </span>
                      </div>
                    </td>

                    {/* Edge with upgraded chip styling */}
                    <td className="px-3 py-2 align-top">
                      <div className="flex flex-col gap-0.5">
                        {(() => {
                          const category = classifyEdge(
                            item.p_home,
                            item.p_away,
                          );

                          const chipText =
                            category === "coinflip"
                              ? "Coin flip"
                              : category === "lean"
                              ? "Modest edge"
                              : "Strong favorite";

                          const chipClass =
                            category === "coinflip"
                              ? "bg-zinc-800 text-zinc-100 border-zinc-600"
                              : category === "lean"
                              ? "bg-amber-500/10 text-amber-300 border-amber-500/60"
                              : "bg-emerald-500/10 text-emerald-300 border-emerald-500/60";

                          return (
                            <span
                              className={
                                "inline-flex items-center rounded-full px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] border " +
                                chipClass
                              }
                            >
                              {chipText}
                            </span>
                          );
                        })()}

                        <span className="text-[10px] text-zinc-500">
                          Diff: {(edge * 100).toFixed(1)}%
                        </span>
                      </div>
                    </td>

                    {/* Odds */}
                    <td className="px-3 py-2 align-top">
                      <div className="flex flex-col gap-0.5">
                        <span className="text-[11px]">
                          Home: {impliedOdds(item.p_home)}
                        </span>
                        <span className="text-[11px]">
                          Away: {impliedOdds(item.p_away)}
                        </span>
                      </div>
                    </td>

                    {/* Logged time */}
                    <td className="px-3 py-2 align-top">
                      <div className="flex flex-col text-[10px] text-zinc-500">
                        <span>{formatIsoToLocal(item.created_at)}</span>
                        <span>{timeAgo(item.created_at)}</span>
                      </div>
                    </td>

                    {/* Fan view link */}
                    <td className="px-3 py-2 align-top text-right">
                      <Link
                        href={`/games/${item.game_id}`}
                        className="inline-flex items-center gap-1 rounded-full border border-blue-500/40 bg-blue-500/10 px-2.5 py-1 text-[10px] font-medium text-blue-200 hover:border-blue-400 hover:bg-blue-500/20 hover:text-blue-50 transition"
                        target="_blank"
                      >
                        <span>Open</span>
                        <span aria-hidden="true">↗</span>
                      </Link>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}