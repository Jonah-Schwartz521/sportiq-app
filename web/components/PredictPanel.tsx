"use client";

import { useState } from "react";
import { api, type PredictResponse } from "@/lib/api";

type Sport = "nba" | "mlb" | "nfl" | "nhl" | "ufc";

export default function PredictPanel() {
  const [sport, setSport] = useState<Sport>("nba");
  const [eventId, setEventId] = useState("");
  const [result, setResult] = useState<PredictResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
      setError(
        err instanceof Error ? err.message : "Failed to fetch prediction"
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="w-full border rounded-lg p-4 bg-zinc-900 text-white">
      <h2 className="text-xl font-semibold mb-2">Predict Win Probability</h2>

      <div className="flex flex-col sm:flex-row gap-3 mb-4">
        <select
          className="bg-zinc-800 p-2 rounded"
          value={sport}
          onChange={(e) => setSport(e.target.value as Sport)}
        >
          <option value="nba">NBA</option>
          <option value="mlb">MLB</option>
          <option value="nfl">NFL</option>
          <option value="nhl">NHL</option>
          <option value="ufc">UFC</option>
        </select>

        <input
          type="number"
          placeholder="event_id"
          className="p-2 bg-zinc-800 rounded flex-1"
          value={eventId}
          onChange={(e) => setEventId(e.target.value)}
        />

        <button
          onClick={handlePredict}
          disabled={loading}
          className="bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 px-4 rounded"
        >
          {loading ? "Loading..." : "Predict"}
        </button>
      </div>

      {error && (
        <p className="text-xs text-red-400 mb-2">
          {error}
        </p>
      )}

      {result && (
        <pre className="text-sm bg-zinc-800 p-3 rounded mt-2 overflow-x-auto">
          {JSON.stringify(result, null, 2)}
        </pre>
      )}

      {!loading && !error && !result && (
        <p className="text-xs text-zinc-500">
          Enter an event_id and hit Predict to see a sample response.
        </p>
      )}
    </div>
  );
}