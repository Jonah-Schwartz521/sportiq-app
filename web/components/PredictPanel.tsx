"use client";

import { useEffect, useMemo, useState } from "react";
import {
  api,
  type EventForPicker,
  type Team,
  type PredictResponse,
} from "@/lib/api";
import {
  sportLabelFromId,
  sportIconFromId,
  sportIdFromKey,
  type SportKey,
} from "@/lib/sport";

function isValidProb(p: number | null | undefined): p is number {
  return typeof p === "number" && !Number.isNaN(p);
}

function safePercent(p: number | null | undefined): string {
  if (!isValidProb(p)) return "–";
  return `${(p * 100).toFixed(1)}%`;
}

// Extended shape for whatever the backend might send back
type PredictBackendResponse = PredictResponse & {
  win_probabilities?: {
    home?: number;
    away?: number;
    home_win?: number;
    away_win?: number;
  };
  probs?: {
    home?: number;
    away?: number;
    home_win?: number;
    away_win?: number;
  };
  prob_home?: number;
  home_prob?: number;
  prob_away?: number;
  away_prob?: number;
  home?: string;
  away?: string;
  home_name?: string;
  away_name?: string;
  game_date?: string;
  event_id?: number;
};

export default function PredictPanel() {
  // Which sport we are predicting (UI only — backend currently supports NBA)
  const [sport, setSport] = useState<SportKey>("nba");

  // Metadata for dropdown
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
          err instanceof Error
            ? err.message
            : "Failed to load events for picker";
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

      // Strongly typed, but with an extended backend shape
      const raw = (await api.predict(finalId as number)) as PredictBackendResponse;

      const probs =
        raw.win_probabilities ?? raw.probs ?? {
          home: raw.p_home,
          away: raw.p_away,
        };

      const pHomeCandidate =
        typeof probs.home === "number"
          ? probs.home
          : typeof probs.home_win === "number"
          ? probs.home_win
          : typeof raw.p_home === "number"
          ? raw.p_home
          : typeof raw.prob_home === "number"
          ? raw.prob_home
          : typeof raw.home_prob === "number"
          ? raw.home_prob
          : undefined;

      const pAwayCandidate =
        typeof probs.away === "number"
          ? probs.away
          : typeof probs.away_win === "number"
          ? probs.away_win
          : typeof raw.p_away === "number"
          ? raw.p_away
          : typeof raw.prob_away === "number"
          ? raw.prob_away
          : typeof raw.away_prob === "number"
          ? raw.away_prob
          : undefined;

      // Normalize into the PredictResponse shape
      const normalized: PredictResponse = {
        game_id: raw.game_id ?? raw.event_id ?? finalId,
        date: raw.date ?? raw.game_date ?? "Unknown date",
        home_team:
          raw.home_team ?? raw.home ?? raw.home_name ?? "Home",
        away_team:
          raw.away_team ?? raw.away ?? raw.away_name ?? "Away",
        p_home: typeof pHomeCandidate === "number" ? pHomeCandidate : NaN,
        p_away: typeof pAwayCandidate === "number" ? pAwayCandidate : NaN,
      };

      setResult(normalized);
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
        <h2 className="text-sm font-semibold text-zinc-100">
          Predict Win Probability
        </h2>
        <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
          POST /predict/nba
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
            <option value="nba">NBA</option>
            <option value="mlb">MLB</option>
            <option value="nfl">NFL</option>
            <option value="nhl">NHL</option>
            <option value="ufc">UFC</option>
          </select>
        </div>

        {/* event dropdown */}
        <div className="flex-1">
          <select
            value={selectedEventId === "" ? "" : String(selectedEventId)}
            onChange={(e) =>
              setSelectedEventId(
                e.target.value ? Number(e.target.value) : "",
              )
            }
            className="w-full bg-zinc-950 border border-zinc-800 rounded px-2 py-1 text-xs"
          >
            <option value="">Pick an event…</option>
            {visibleEvents.map((e) => (
              <option key={e.event_id} value={e.event_id}>
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
            Game{" "}
            <span className="font-mono">
              #{result.game_id} — {result.away_team || "Away"} @{" "}
              {result.home_team || "Home"}
            </span>
          </div>
          <div>Date: {result.date}</div>

          {isValidProb(result.p_home) || isValidProb(result.p_away) ? (
            <div>
              Home win prob:{" "}
              <span className="text-zinc-100">
                {safePercent(result.p_home)}
              </span>{" "}
              · Away win prob:{" "}
              <span className="text-zinc-100">
                {safePercent(result.p_away)}
              </span>
            </div>
          ) : (
            <div className="text-zinc-500">
              Model probabilities unavailable for this event.
            </div>
          )}
        </div>
      )}
    </div>
  );
}