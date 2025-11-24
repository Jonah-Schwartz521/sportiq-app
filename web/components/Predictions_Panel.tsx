"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  api,
  type PredictionLogItem,
  type PredictionLogResponse,
} from "@/lib/api";

// Edge filter buckets for the pills
type EdgeFilter = "all" | "coinflip" | "lean" | "strong";

function classifyEdge(pHome: number, pAway: number): EdgeFilter {
  const edge = Math.abs(pHome - pAway);
  if (edge < 0.05) return "coinflip";     // near coin flip
  if (edge < 0.15) return "lean";         // modest edge
  return "strong";                        // strong favorite
}

function edgeLabelFromFilter(filter: EdgeFilter): string {
  if (filter === "coinflip") return "Coin flip";
  if (filter === "lean") return "Modest edge";
  if (filter === "strong") return "Strong edge";
  return "All edges";
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

  // Load recent predictions from FastAPI /predictions
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

  // Apply edge filter
  const filtered = useMemo(() => {
    if (filter === "all") return data;
    return data.filter((i) => classifyEdge(i.p_home, i.p_away) === filter);
  }, [data, filter]);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-2">
        <div>
          <h2 className="text-sm font-semibold text-zinc-100">
            Recent Predictions
          </h2>
          <p className="text-[11px] text-zinc-500">
            Latest calls from the NBA model (logged from /predict_by_game_id).
          </p>
        </div>

        {/* Filter pills */}
        <div className="flex items-center gap-1">
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
                "px-2 py-1 rounded-full text-[10px] uppercase tracking-[0.14em] " +
                (filter === f.key
                  ? "bg-zinc-200 text-black"
                  : "bg-zinc-900 text-zinc-400 hover:bg-zinc-800")
              }
            >
              {f.label}
            </button>
          ))}
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
        <div className="overflow-x-auto rounded-xl border border-zinc-800 bg-zinc-950/60">
          <table className="w-full text-xs text-zinc-300">
            <thead className="text-[10px] uppercase tracking-[0.16em] text-zinc-500 bg-zinc-950 border-b border-zinc-800">
              <tr>
                <th className="px-3 py-2 text-left">Matchup</th>
                <th className="px-3 py-2 text-left">Probs</th>
                <th className="px-3 py-2 text-left">Edge</th>
                <th className="px-3 py-2 text-left">Odds (home/away)</th>
                <th className="px-3 py-2 text-left">Logged</th>
                <th className="px-3 py-2 text-right">Fan view</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((item) => {
                const edge = Math.abs(item.p_home - item.p_away);
                const edgeBucket = classifyEdge(item.p_home, item.p_away);

                return (
                  <tr
                    key={`${item.game_id}-${item.created_at}`}
                    className="border-t border-zinc-900/80"
                  >
                    <td className="px-3 py-2">
                      <div className="flex flex-col">
                        <span className="font-medium text-zinc-100">
                          {item.away_team} @ {item.home_team}
                        </span>
                        <span className="text-[10px] text-zinc-500">
                          Game ID {item.game_id} · {item.date}
                        </span>
                      </div>
                    </td>

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

                    <td className="px-3 py-2 align-top">
                      <div className="flex flex-col gap-0.5">
                        <span className="inline-flex items-center rounded-full border border-zinc-700 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em]">
                          {edgeLabelFromFilter(edgeBucket)}
                        </span>
                        <span className="text-[10px] text-zinc-500">
                          Diff: {(edge * 100).toFixed(1)}%
                        </span>
                      </div>
                    </td>

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

                    <td className="px-3 py-2 align-top">
                      <span className="text-[10px] text-zinc-500">
                        {item.created_at}
                      </span>
                    </td>

                    <td className="px-3 py-2 align-top text-right">
                      <Link
                        href={`/games/${item.game_id}`}
                        className="text-[11px] text-blue-400 hover:text-blue-300 underline underline-offset-2"
                        target="_blank"
                      >
                        Open
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