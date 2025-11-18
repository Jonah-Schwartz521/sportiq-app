"use client";

import { useEffect, useMemo, useState } from "react";
import { api, type EventForPicker, type Team, type PredictResponse } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";

type SportKey = "nba" | "mlb" | "nfl" | "nhl" | "ufc";

const sportOptions: { id: SportKey; label: string }[] = [
  { id: "nba", label: "NBA" },
  { id: "mlb", label: "MLB" },
  { id: "nfl", label: "NFL" },
  { id: "nhl", label: "NHL" },
  { id: "ufc", label: "UFC" },
];

// map backend sport slug -> numeric sport_id in your data
function sportIdFromKey(key: SportKey): number {
  switch (key) {
    case "nba":
      return 1;
    case "mlb":
      return 2;
    case "nfl":
      return 3;
    case "nhl":
      return 4;
    case "ufc":
      return 5;
  }
}

function PredictPanel() {
  const [sport, setSport] = useState<SportKey>("nba");

  const [events, setEvents] = useState<EventForPicker[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loadingEvents, setLoadingEvents] = useState(true);
  const [eventsError, setEventsError] = useState<string | null>(null);

  const [selectedEventId, setSelectedEventId] = useState<number | "">("");
  const [manualEventId, setManualEventId] = useState<string>("");

  const [result, setResult] = useState<PredictResponse | null>(null);
  const [predictLoading, setPredictLoading] = useState(false);
  const [predictError, setPredictError] = useState<string | null>(null);

  // ---- Teams lookup + helpers ----
  const teamsById = useMemo(() => {
    const map = new Map<number, Team>();
    for (const t of teams) {
      map.set(t.team_id, t);
    }
    return map;
  }, [teams]);

  function teamLabel(id: number | null): string {
    if (id == null) return "TBD";
    const team = teamsById.get(id);
    if (!team) return `#${id}`;
    return team.name;
  }

  function eventLabel(e: EventForPicker): string {
    const home = teamLabel(e.home_team_id);
    const away = teamLabel(e.away_team_id);
    const sportText = sportLabelFromId(e.sport_id);
    return `${sportText} · ${away} @ ${home} · ${e.date}`;
  }

  // ---- Load events + teams for the picker ----
  useEffect(() => {
    (async () => {
      try {
        setLoadingEvents(true);
        setEventsError(null);

        const [eventsRes, teamsRes] = await Promise.all([
          api.eventsForPicker(),
          api.teams(),
        ]);

        setEvents(eventsRes.items || []);
        setTeams(teamsRes.items || []);
      } catch (err: unknown) {
        console.error(err);
        const msg =
          err instanceof Error ? err.message : "Failed to load events for picker";
        setEventsError(msg);
      } finally {
        setLoadingEvents(false);
      }
    })();
  }, []);

  // Filter events by selected sport so dropdown is shorter
  const visibleEvents = useMemo(() => {
    const id = sportIdFromKey(sport);
    return events.filter((e) => e.sport_id === id);
  }, [events, sport]);

  // ---- Predict handler ----
  async function handlePredict() {
    try {
      setPredictLoading(true);
      setPredictError(null);
      setResult(null);

      const finalId =
        selectedEventId !== "" ? selectedEventId : Number(manualEventId || 0);

      if (!finalId) {
        setPredictError("Please pick an event or enter a valid event_id.");
        return;
      }

      const res = await api.predict(sport, finalId);
      setResult(res);
    } catch (err: unknown) {
      console.error(err);
      const msg =
        err instanceof Error ? err.message : "Prediction request failed";
      setPredictError(msg);
    } finally {
      setPredictLoading(false);
    }
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between mb-1">
        <h2 className="text-sm font-semibold text-zinc-100">Predict Win Probability</h2>
        <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
          POST /predict/&lt;sport&gt;
        </span>
      </div>

      <div className="flex flex-col sm:flex-row gap-2 items-stretch sm:items-center">
        {/* sport select */}
        <div className="flex items-center gap-1 text-xs">
          <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
            Sport
          </span>
          <select
            value={sport}
            onChange={(e) => {
              setSport(e.target.value as SportKey);
              setSelectedEventId("");
            }}
            className="bg-zinc-950 border border-zinc-800 rounded px-2 py-1 text-xs"
          >
            {sportOptions.map((s) => (
              <option key={s.id} value={s.id}>
                {s.label}
              </option>
            ))}
          </select>
        </div>

        {/* event dropdown */}
        <div className="flex-1">
          <select
            value={selectedEventId === "" ? "" : String(selectedEventId)}
            onChange={(e) =>
              setSelectedEventId(e.target.value ? Number(e.target.value) : "")
            }
            className="w-full bg-zinc-950 border border-zinc-800 rounded px-2 py-1 text-xs"
          >
            <option value="">Pick an event…</option>
            {visibleEvents.map((e) => (
              <option key={e.event_id} value={e.event_id}>
                {/* Icon + label to match fan UI */}
                {sportIconFromId(e.sport_id)} {eventLabel(e)}
              </option>
            ))}
          </select>
          {loadingEvents && (
            <p className="text-[10px] text-zinc-500 mt-1">Loading events…</p>
          )}
          {eventsError && (
            <p className="text-[10px] text-red-400 mt-1">{eventsError}</p>
          )}
        </div>

        {/* manual event id input */}
        <div className="flex items-center gap-1 text-xs">
          <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
            event_id
          </span>
          <input
            value={manualEventId}
            onChange={(e) => setManualEventId(e.target.value)}
            placeholder="optional"
            className="w-20 bg-zinc-950 border border-zinc-800 rounded px-2 py-1 text-xs"
          />
        </div>

        <button
          onClick={handlePredict}
          disabled={predictLoading}
          className="text-xs px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 disabled:opacity-60"
        >
          {predictLoading ? "Predicting…" : "Predict"}
        </button>
      </div>

      {/* result / error */}
      {predictError && (
        <p className="text-xs text-red-400 mt-1">{predictError}</p>
      )}

      {result && !predictError && (
        <div className="mt-2 text-[11px] text-zinc-400 space-y-1">
          <div>
            Model: <span className="font-mono">{result.model_key}</span>
          </div>
          <div>Generated: {result.generated_at}</div>
          <div>
            Home:{" "}
            {((result.win_probabilities.home ?? 0) * 100).toFixed(1)}% · Away:{" "}
            {((result.win_probabilities.away ?? 0) * 100).toFixed(1)}%
          </div>
        </div>
      )}
    </div>
  );
}

export default PredictPanel;