"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type Team } from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";

// Local types matching what we expect from the backend / API
type PredictionSummary = {
  event_id: number;
  model_key: string;
  home_wp: number;
  away_wp: number;
  created_at: string;
};

type EventForPicker = {
  event_id: number;
  sport_id: number;
  date: string;
  home_team_id: number | null;
  away_team_id: number | null;
};

export default function PredictionsPanel() {
  const [predictions, setPredictions] = useState<PredictionSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [events, setEvents] = useState<EventForPicker[]>([]);
  const [teams, setTeams] = useState<Team[]>([]);
  const [metaError, setMetaError] = useState<string | null>(null);

  // ---- Load predictions ----
  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);

        // api.predictions is currently mocked; cast to our local type
        const res = await api.predictions();
        const items = (res.items || []) as PredictionSummary[];
        setPredictions(items);
      } catch (err: unknown) {
        console.error(err);
        const msg =
          err instanceof Error ? err.message : "Failed to load predictions";
        setError(msg);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  // ---- Load events + teams so we can show matchups ----
  useEffect(() => {
    (async () => {
      try {
        const [eventsRes, teamsRes] = await Promise.all([
          api.events(), // we can reuse /events as the picker source
          api.teams(),
        ]);

        setEvents((eventsRes.items || []) as EventForPicker[]);
        setTeams(teamsRes.items || []);
      } catch (err: unknown) {
        console.error(err);
        const msg =
          err instanceof Error ? err.message : "Failed to load metadata";
        setMetaError(msg);
      }
    })();
  }, []);

  // ---- Lookup maps + helpers ----
  const eventsById = useMemo(() => {
    const map = new Map<number, EventForPicker>();
    for (const e of events) {
      map.set(e.event_id, e);
    }
    return map;
  }, [events]);

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

  function eventLabelFromPrediction(p: PredictionSummary): {
    sportId: number | null;
    sportText: string;
    icon: string;
    matchup: string;
    dateText: string | null;
  } {
    const e = eventsById.get(p.event_id);
    if (!e) {
      return {
        sportId: null,
        sportText: "Unknown",
        icon: "🏟️",
        matchup: `Event #${p.event_id}`,
        dateText: null,
      };
    }

    const sportText = sportLabelFromId(e.sport_id);
    const icon = sportIconFromId(e.sport_id);
    const home = teamLabel(e.home_team_id);
    const away = teamLabel(e.away_team_id);

    return {
      sportId: e.sport_id,
      sportText,
      icon,
      matchup: `${away} @ ${home}`,
      dateText: e.date,
    };
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between mb-1">
        <h2 className="text-sm font-semibold text-zinc-100">
          Recent Predictions
        </h2>
        <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
          GET /predictions
        </span>
      </div>

      {loading && (
        <p className="text-xs text-zinc-500">Loading recent predictions…</p>
      )}

      {error && !loading && (
        <p className="text-xs text-red-400">{error}</p>
      )}

      {!loading && !error && predictions.length === 0 && (
        <p className="text-xs text-zinc-500">
          No predictions have been recorded yet.
        </p>
      )}

      {metaError && (
        <p className="text-[10px] text-yellow-400">
          {metaError} — showing predictions without full matchup context.
        </p>
      )}

      {!loading && !error && predictions.length > 0 && (
        <ul className="space-y-2 text-xs text-zinc-300">
          {predictions.map((p) => {
            const meta = eventLabelFromPrediction(p);
            const homePct = (p.home_wp * 100).toFixed(1);
            const awayPct = (p.away_wp * 100).toFixed(1);

            return (
              <li
                key={`${p.event_id}-${p.created_at}-${p.model_key}`}
                className="rounded-xl border border-zinc-800 bg-zinc-950/60 px-3 py-2"
              >
                {/* Top line: sport + matchup */}
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <span className="text-[13px]">
                      {meta.icon}
                    </span>
                    <div className="flex flex-col">
                      <span className="text-[11px] uppercase tracking-[0.16em] text-zinc-500">
                        {meta.sportText}
                      </span>
                      <span className="text-xs text-zinc-100">
                        {meta.matchup}
                      </span>
                    </div>
                  </div>

                  <div className="text-right text-[10px] text-zinc-500">
                    <div className="font-mono truncate max-w-[160px]">
                      {p.model_key}
                    </div>
                    <div>{p.created_at}</div>
                  </div>
                </div>

                {/* Probabilities */}
                <div className="mt-2 flex items-center justify-between text-[11px]">
                  <div className="flex flex-col">
                    <span className="text-zinc-400">
                      Home win prob
                    </span>
                    <span className="font-medium text-zinc-100">
                      {homePct}%
                    </span>
                  </div>
                  <div className="flex flex-col">
                    <span className="text-zinc-400">
                      Away win prob
                    </span>
                    <span className="font-medium text-zinc-100">
                      {awayPct}%
                    </span>
                  </div>
                  <div className="flex flex-col items-end">
                    {meta.dateText && (
                      <span className="text-[10px] text-zinc-500 mb-0.5">
                        {meta.dateText}
                      </span>
                    )}
                    <Link
                      href={`/games/${p.event_id}`}
                      className="text-[11px] text-blue-400 hover:text-blue-300"
                    >
                      Open fan view →
                    </Link>
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}