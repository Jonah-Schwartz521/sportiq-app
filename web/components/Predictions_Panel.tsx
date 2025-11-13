"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";

type PredictionRow = {
  event_id: number;
  model_key: string;
  home_wp: number;
  away_wp: number;
  created_at: string;
};

export default function PredictionsPanel() {
  const [rows, setRows] = useState<PredictionRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const data = await api.predictions();
        setRows(data.items || []);
      } catch (err: any) {
        console.error(err);
        setError(err.message || "Failed to load predictions");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <div className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4">
      <div className="flex items-center justify-between mb-2">
        <h2 className="text-sm font-medium text-zinc-200">
          Recent Predictions
        </h2>
        <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
          GET /predictions
        </span>
      </div>

      {loading ? (
        <p className="text-xs text-zinc-500">Loading…</p>
      ) : error ? (
        <p className="text-xs text-red-400">{error}</p>
      ) : rows.length === 0 ? (
        <p className="text-xs text-zinc-500">
          No predictions recorded yet. Hit the Predict button above.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-left text-zinc-300">
            <thead className="text-[10px] uppercase text-zinc-500 border-b border-zinc-900">
              <tr>
                <th className="py-1 pr-2">Event</th>
                <th className="py-1 pr-2">Model</th>
                <th className="py-1 pr-2">Home WP</th>
                <th className="py-1 pr-2">Away WP</th>
                <th className="py-1">Created</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((p) => (
                <tr key={`${p.event_id}-${p.model_key}-${p.created_at}`} className="border-b border-zinc-900/60 last:border-b-0">
                  <td className="py-1 pr-2 text-zinc-200">#{p.event_id}</td>
                  <td className="py-1 pr-2 text-zinc-400 truncate max-w-[120px]">
                    {p.model_key}
                  </td>
                  <td className="py-1 pr-2">
                    {(p.home_wp * 100).toFixed(1)}%
                  </td>
                  <td className="py-1 pr-2">
                    {(p.away_wp * 100).toFixed(1)}%
                  </td>
                  <td className="py-1 text-zinc-500 truncate max-w-[130px]">
                    {new Date(p.created_at).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}