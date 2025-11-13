"use client";

import { useEffect, useState } from "react";
import {
  api,
  type PredictResponse,
  type EventForPicker,
} from "@/lib/api";

type Sport = "nba" | "mlb" | "nfl" | "nhl" | "ufc";

export default function PredictPanel() {
  const [sport, setSport] = useState<Sport>("nba");
  const [eventId, setEventId] = useState<string>(""); // always string for the input
  const [result, setResult] = useState<PredictResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [events, setEvents] = useState<EventForPicker[]>([]);
  const [loadingEvents, setLoadingEvents] = useState(false);

  // Load events for dropdown on mount
  useEffect(() => {
    (async () => {
      try {
        setLoadingEvents(true);
        const data = await api.eventsForPicker();
        setEvents(data.items || []);
      } catch (e) {
        console.error(e);
      } finally {
        setLoadingEvents(false);
      }
    })();
  }, []);

  async function handlePredict() {
    setLoading(true);
    setError(null);
    setResult(null);

    const idNum = Number(eventId);
    if (!idNum || Number.isNaN(idNum)) {
      setError("Please enter a valid numeric event_id");
      setLoading(false);
      return;
    }

    try {
      const data = await api.predict(sport, idNum);
      setResult(data);
    } catch (err: unknown) {
      console.error(err);
      const message =
        err instanceof Error ? err.message : "Failed to fetch prediction";
      setError(message);
    } finally {
      setLoading(false);
    }
  }

  function handleSelectChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const selected = e.target.value;
    // value is the event_id as string
    setEventId(selected);
  }

  return (
    <div className="w-full rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4 space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-medium text-zinc-200">
          Predict Win Probability
        </h2>
        <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
          POST /predict/&lt;sport&gt;
        </span>
      </div>

      {/* Controls */}
      <div className="flex flex-col gap-3 sm:flex-row">
        <select
          className="bg-zinc-900 border border-zinc-800 rounded px-3 py-2 text-sm"
          value={sport}
          onChange={(e) => setSport(e.target.value as Sport)}
        >
          <option value="nba">NBA</option>
          <option value="mlb">MLB</option>
          <option value="nfl">NFL</option>
          <option value="nhl">NHL</option>
          <option value="ufc">UFC</option>
        </select>

        {/* Event dropdown */}
        <select
          className="bg-zinc-900 border border-zinc-800 rounded px-3 py-2 text-sm flex-1"
          value={eventId} // keep it controlled
          onChange={handleSelectChange}
        >
          <option value="">
            {loadingEvents ? "Loading events…" : "Pick an event…"}
          </option>
          {events.map((ev) => (
            <option key={ev.event_id} value={String(ev.event_id)}>
              {`Event ${ev.event_id} • ${ev.date} • home ${ev.home_team_id ?? "-"} vs away ${ev.away_team_id ?? "-"}`}
            </option>
          ))}
        </select>

        {/* Manual input (always controlled) */}
        <input
          type="number"
          placeholder="event_id"
          className="bg-zinc-900 border border-zinc-800 rounded px-3 py-2 text-sm flex-1"
          value={eventId}
          onChange={(e) => setEventId(e.target.value)}
        />

        <button
          onClick={handlePredict}
          disabled={loading}
          className="bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 rounded px-4 py-2 text-sm font-medium"
        >
          {loading ? "Loading…" : "Predict"}
        </button>
      </div>

      <p className="text-[11px] text-zinc-500">
        Pick an event from the dropdown or enter an{" "}
        <span className="font-mono">event_id</span> manually, then hit{" "}
        <span className="font-mono">Predict</span>.
      </p>

      {/* Status / result */}
      {error && <p className="text-xs text-red-400">{error}</p>}

      {result && (
        <div className="space-y-2">
          {/* Friendly summary */}
          <div className="text-sm text-zinc-200">
            {typeof result.win_probabilities.home === "number" &&
            typeof result.win_probabilities.away === "number" ? (
              <>
                Home:{" "}
                <span className="font-mono">
                  {(result.win_probabilities.home * 100).toFixed(1)}%
                </span>{" "}
                · Away:{" "}
                <span className="font-mono">
                  {(result.win_probabilities.away * 100).toFixed(1)}%
                </span>
              </>
            ) : (
              <span className="text-zinc-400">
                Model returned non-standard probabilities.
              </span>
            )}
          </div>

          {/* Raw JSON for debugging */}
          <pre className="text-xs bg-zinc-900/80 p-3 rounded overflow-x-auto">
            {JSON.stringify(result, null, 2)}
          </pre>
        </div>
      )}

      {!loading && !error && !result && (
        <p className="text-xs text-zinc-500">
          No prediction yet. Choose an event and click Predict.
        </p>
      )}
    </div>
  );
}