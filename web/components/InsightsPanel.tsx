"use client";

import { useState } from "react";
import { api, type Insight } from "@/lib/api";

// Reuse the return type of api.insights so it always stays in sync
type InsightsResponse = Awaited<ReturnType<typeof api.insights>>;

export default function InsightsPanel() {
  const [eventId, setEventId] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<InsightsResponse | null>(null);

  async function handleFetch() {
    setLoading(true);
    setError(null);
    setData(null);

    const idNum = Number(eventId);
    if (!idNum || Number.isNaN(idNum)) {
      setError("Please enter a valid numeric event_id");
      setLoading(false);
      return;
    }

    try {
      // api.insights now takes just eventId
      const res = await api.insights(idNum);
      setData(res);
    } catch (err: unknown) {
      console.error(err);
      const message =
        err instanceof Error ? err.message : "Failed to load insights";
      setError(message);
    } finally {
      setLoading(false);
    }
  }

  // Prefer game_id, fall back to event_id if that’s what the API returns
  const displayGameId =
    data && (data.game_id ?? (data as any).event_id ?? null);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-sm font-medium text-zinc-200">
            Explain a Prediction
          </h2>
          <p className="text-xs text-zinc-500">
            Fetch top reasons from{" "}
            <span className="font-mono">
              /insights/nba/{"{event_id}"}
            </span>
            .
          </p>
        </div>
        <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
          GET /insights/nba/&#123;event_id&#125;
        </span>
      </div>

      {/* Controls */}
      <div className="flex flex-col sm:flex-row gap-3">
        <input
          type="number"
          placeholder="event_id (game_id)"
          className="bg-zinc-900 border border-zinc-800 rounded px-3 py-2 text-sm flex-1"
          value={eventId}
          onChange={(e) => setEventId(e.target.value)}
        />

        <button
          onClick={handleFetch}
          disabled={loading}
          className="bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 rounded px-4 py-2 text-sm font-medium"
        >
          {loading ? "Loading…" : "Fetch Insights"}
        </button>
      </div>

      {/* Status / results */}
      {error && <p className="text-xs text-red-400">{error}</p>}

      {loading && !error && (
        <p className="text-xs text-zinc-500">Loading insights…</p>
      )}

      {data && (
        <div className="border border-zinc-800 rounded-xl px-4 py-3 space-y-3 bg-black/40">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="text-xs text-zinc-400">
              Game{" "}
              <span className="text-zinc-200 font-mono">
                {displayGameId ?? "unknown"}
              </span>
              <br />
              Model{" "}
              <span className="text-zinc-200 font-mono">
                {data.model_key}
              </span>
            </div>
            <div className="text-right text-[10px] text-zinc-500 space-y-0.5">
              <div>
                Generated at{" "}
                {new Date(data.generated_at).toLocaleString()}
              </div>
            </div>
          </div>

          {data.insights.length > 0 ? (
            <ul className="space-y-2">
              {data.insights.map((insight, idx) => (
                <li
                  key={`${insight.type}-${insight.label}-${idx}`}
                  className="rounded-lg border border-zinc-800 bg-zinc-950/80 px-3 py-2"
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs font-medium text-zinc-200">
                      {insight.label}
                    </span>
                    {typeof insight.value === "number" && (
                      <span className="text-[10px] text-zinc-500">
                        {(insight.value * 100).toFixed(1)}%
                      </span>
                    )}
                  </div>
                  <p className="text-[11px] text-zinc-400">
                    {insight.detail}
                  </p>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-zinc-500">
              No insights available yet for this event.
            </p>
          )}
        </div>
      )}

      {!loading && !error && !data && (
        <p className="text-xs text-zinc-500">
          Enter an event_id (game_id) to see explanation reasons.
        </p>
      )}
    </div>
  );
}