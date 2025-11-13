"use client";

import { useState } from "react";
import { api } from "@/lib/api";

export default function PredictPanel() {
  const [sport, setSport] = useState("nba");
  const [eventId, setEventId] = useState("");
  const [result, setResult] = useState<any | null>(null);
  const [loading, setLoading] = useState(false);

  async function handlePredict() {
    setLoading(true);
    setResult(null);

    const data = await api.predict(sport as any, Number(eventId));
    setResult(data);

    setLoading(false);
  }

  return (
    <div className="w-full border rounded-lg p-4 bg-zinc-900 text-white">
      <h2 className="text-xl font-semibold mb-2">Predict Win Probability</h2>

      <div className="flex gap-4 mb-4">
        <select
          className="bg-zinc-800 p-2 rounded"
          value={sport}
          onChange={(e) => setSport(e.target.value)}
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
          className="p-2 bg-zinc-800 rounded"
          value={eventId}
          onChange={(e) => setEventId(e.target.value)}
        />

        <button
          onClick={handlePredict}
          className="bg-blue-600 hover:bg-blue-700 px-4 rounded"
        >
          {loading ? "Loading..." : "Predict"}
        </button>
      </div>

      {result && (
        <pre className="text-sm bg-zinc-800 p-3 rounded mt-4">
          {JSON.stringify(result, null, 2)}
        </pre>
      )}
    </div>
  );
}