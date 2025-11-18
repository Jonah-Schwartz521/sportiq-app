"use client";

import { useEffect, useMemo, useState } from "react";
import {
  api,
  type EventForPicker,
  type Team,
  type PredictResponse,
} from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";

type SportKey = "nba" | "mlb" | "nfl" | "nhl" | "ufc";

const SPORT_OPTIONS: { key: SportKey; label: string }[] = [
  { key: "nba", label: "NBA" },
  { key: "mlb", label: "MLB" },
  { key: "nfl", label: "NFL" },
  { key: "nhl", label: "NHL" },
  { key: "ufc", label: "UFC" },
];

// Map sport slug -> internal numeric sport_id (from your API)
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
    default:
      return 1;
  }
}

export default function PredictPanel() {
  // which sport we’re predicting
  const [sportKey, setSportKey] = useState<SportKey>("nba");

  // metadata for dropdown
  const [events, setEvents] = useState<EventForPicker[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [metaLoading, setMetaLoading] = useState(true);
  const [metaError, setMetaError] = useState<string | null>(null);

  // selected event
  const [selectedEventId, setSelectedEventId] = useState<number | "">("");

  // prediction response
  const [prediction, setPrediction] = useState<PredictResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ---- Load events + teams for the dropdown ----
  useEffect(() => {
    (async () => {
      try {
        setMetaLoading(true);
        setMetaError(null);

        const [eventsRes, teamsRes] = await Promise.all([
          api.eventsForPicker(),
          api.teams(),
        ]);

        setEvents(eventsRes.items || []);
        setTeams(teamsRes.items || []);
      } catch (err: unknown) {
        console.error(err);
        const msg =
          err instanceof Error ? err.message : "Failed to load events/teams";
        setMetaError(msg);
      } finally {
        setMetaLoading(false);
      }
    })();
  }, []);

  // ---- Lookups + helpers ----
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

  // filter events by currently selected sport
  const visibleEvents = useMemo(() => {
    const targetSportId = sportIdFromKey(sportKey);
    return events.filter((e) => e.sport_id === targetSportId);
  }, [events, sportKey]);

  function eventOptionLabel(e: EventForPicker): string {
    const home = teamLabel(e.home_team_id);
    const away = teamLabel(e.away_team_id);
    const icon = sportIconFromId(e.sport_id);
    const sport = sportLabelFromId(e.sport_id);

    return `${icon} ${sport} · ${away} @ ${home} · ${e.date}`;
  }

  // ---- Submit handler ----
  async function handleSubmit(evt: React.FormEvent) {
    evt.preventDefault();

    const eid =
      typeof selectedEventId === "string"
        ? Number(selectedEventId)
        : selectedEventId;

    if (!eid || Number.isNaN(eid)) {
      setError("Please select an event.");
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setPrediction(null);

      const res = await api.predict(sportKey, eid);
      console.log("PredictPanel → predict response", res);
      setPrediction(res);
    } catch (err: unknown) {
      console.error(err);
      const msg =
        err instanceof Error ? err.message : "Prediction request failed";
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between mb-1">
        <h2 className="text-sm font-semibold text-zinc-100">
          Predict Win Probability
        </h2>
        <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
          POST /predict/:sport
        </span>
      </div>

      {/* Sport picker */}
      <form onSubmit={handleSubmit} className="space-y-3 text-xs">
        <div className="flex flex-wrap gap-2">
          {SPORT_OPTIONS.map((opt) => (
            <button
              key={opt.key}
              type="button"
              onClick={() => setSportKey(opt.key)}
              className={
                "px-2 py-1 rounded-full border text-[11px] " +
                (sportKey === opt.key
                  ? "border-blue-500/80 bg-blue-500/10 text-blue-100"
                  : "border-zinc-700 text-zinc-400 hover:border-zinc-500")
              }
            >
              {opt.label}
            </button>
          ))}
        </div>

        {/* Event dropdown */}
        <div className="space-y-1">
          <label className="block text-[11px] text-zinc-500 uppercase tracking-[0.16em]">
            Event
          </label>

          {metaLoading ? (
            <p className="text-[11px] text-zinc-500">
              Loading events and teams…
            </p>
          ) : metaError ? (
            <p className="text-[11px] text-red-400">{metaError}</p>
          ) : visibleEvents.length === 0 ? (
            <p className="text-[11px] text-zinc-500">
              No events available for this sport.
            </p>
          ) : (
            <select
              className="w-full bg-black border border-zinc-700 rounded-md px-2 py-1 text-xs text-zinc-100"
              value={selectedEventId}
              onChange={(e) => {
                const val = e.target.value;
                setSelectedEventId(val === "" ? "" : Number(val));
              }}
            >
              <option value="">Select an event…</option>
              {visibleEvents.map((e) => (
                <option key={e.event_id} value={e.event_id}>
                  {eventOptionLabel(e)}
                </option>
              ))}
            </select>
          )}
        </div>

        {/* Submit */}
        <button
          type="submit"
          disabled={loading || metaLoading || !visibleEvents.length}
          className="mt-1 inline-flex items-center justify-center rounded-md bg-blue-600 hover:bg-blue-500 disabled:bg-zinc-700 disabled:text-zinc-400 px-3 py-1 text-[11px] font-medium"
        >
          {loading ? "Predicting…" : "Run prediction"}
        </button>

        {/* Error */}
        {error && (
          <p className="text-[11px] text-red-400 mt-1">{error}</p>
        )}
      </form>

      {/* Prediction result */}
      <div className="border border-zinc-800 rounded-xl bg-zinc-950/60 px-3 py-2 text-xs text-zinc-200 space-y-1">
        {loading && (
          <p className="text-[11px] text-zinc-500">Waiting for model…</p>
        )}

        {!loading && !prediction && !error && (
          <p className="text-[11px] text-zinc-500">
            Select a sport and event, then run a prediction to see the model
            output here.
          </p>
        )}

        {prediction && !loading && !error && (
          <>
            <div className="flex items-center justify-between text-[11px] text-zinc-400">
              <span>
                Model:{" "}
                <span className="font-mono">{prediction.model_key}</span>
              </span>
              <span>Generated: {prediction.generated_at}</span>
            </div>
            <div className="mt-1 flex items-center justify-between text-[11px]">
              <div className="flex flex-col">
                <span className="text-zinc-400">Home win prob</span>
                <span className="font-medium text-zinc-100">
                  {(
                    (prediction.win_probabilities.home ?? 0) * 100
                  ).toFixed(1)}
                  %
                </span>
              </div>
              <div className="flex flex-col">
                <span className="text-zinc-400">Away win prob</span>
                <span className="font-medium text-zinc-100">
                  {(
                    (prediction.win_probabilities.away ?? 0) * 100
                  ).toFixed(1)}
                  %
                </span>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}