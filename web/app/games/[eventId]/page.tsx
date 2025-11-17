"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import {
  api,
  type Event,
  type Team,
  type PredictResponse,
  type Insight,
} from "@/lib/api";
import { sportLabelFromId, sportIconFromId } from "@/lib/sport";

// Helper to map sport_id -> backend slug for /predict/:sport and /insights/:sport
function sportKeyFromId(id: number | null): string {
  switch (id) {
    case 1:
      return "nba";
    case 2:
      return "mlb";
    case 3:
      return "nfl";
    case 4:
      return "nhl";
    case 5:
      return "ufc";
    default:
      return "nba";
  }
}

export default function GameDetailPage() {
  // Because the folder is [eventId], the param key is "eventId"
  const { eventId: eventIdParam } = useParams() as { eventId: string };
  const eventId = Number(eventIdParam);

  const [event, setEvent] = useState<Event | null>(null);
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Model prediction state
  const [prediction, setPrediction] = useState<PredictResponse | null>(null);
  const [predLoading, setPredLoading] = useState(false);
  const [predError, setPredError] = useState<string | null>(null);

  // Insights state
  const [insights, setInsights] = useState<Insight[] | null>(null);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);

  // Fallback timestamp for when the page rendered
  const [generatedAt] = useState(() => new Date().toISOString());

  // Fetch this event + all teams
  useEffect(() => {
    if (!eventId) return;

    (async () => {
      try {
        setLoading(true);
        setError(null);

        const [eventsRes, teamsRes] = await Promise.all([
          api.events(),
          api.teams(),
        ]);

        const events = eventsRes.items || [];
        const found = events.find((e) => e.event_id === eventId) ?? null;

        if (!found) {
          setError("Game not found");
          setEvent(null);
        } else {
          setEvent(found);
        }

        setTeams(teamsRes.items || []);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load game";
        setError(message);
      } finally {
        setLoading(false);
      }
    })();
  }, [eventId]);

  // Build team lookup
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

  const homeName = event ? teamLabel(event.home_team_id) : "";
  const awayName = event ? teamLabel(event.away_team_id) : "";

  // Fetch a prediction once we know the event
  useEffect(() => {
    if (!event) return;

    (async () => {
      try {
        setPredLoading(true);
        setPredError(null);
        setPrediction(null);

        const sportKey = sportKeyFromId(event.sport_id);
        const result = await api.predict(sportKey, event.event_id);

        console.log("Predict response", result);
        setPrediction(result);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load prediction";
        setPredError(message);
      } finally {
        setPredLoading(false);
      }
    })();
  }, [event]);

  // 🔹 Fetch insights once we know the event
  useEffect(() => {
    if (!event) return;

    (async () => {
      try {
        setInsightsLoading(true);
        setInsightsError(null);
        setInsights(null);

        const sportKey = sportKeyFromId(event.sport_id);
        const data = await api.insights(sportKey, event.event_id);

        console.log("Insights response", data);
        setInsights(data.insights || []);
      } catch (err: unknown) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to load insights";
        setInsightsError(message);
      } finally {
        setInsightsLoading(false);
      }
    })();
  }, [event]);

  return (
    <main className="min-h-screen bg-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-4xl space-y-6">
        {/* Top bar */}
        <header className="flex items-center justify-between gap-2">
          <Link
            href="/games"
            className="text-xs text-zinc-400 hover:text-zinc-200 transition-colors"
          >
            ← Back to games
          </Link>

          <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
            Game Detail
          </span>
        </header>

        {/* Loading / error */}
        {loading && (
          <p className="text-sm text-zinc-500">Loading game details…</p>
        )}

        {error && !loading && (
          <p className="text-sm text-red-400">{error}</p>
        )}

        {!loading && !error && !event && (
          <p className="text-sm text-zinc-500">Game not found.</p>
        )}

        {/* Main content */}
        {event && (
          <div className="space-y-6">
            {/* Matchup header */}
            <section className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-xs text-zinc-400">
                  <span>{sportIconFromId(event.sport_id)}</span>
                  <span className="uppercase tracking-[0.16em]">
                    {sportLabelFromId(event.sport_id)}
                  </span>
                  <span className="inline-flex items-center rounded-full border border-zinc-700 px-2 py-0.5 text-[10px] uppercase tracking-[0.16em]">
                    {event.status || "scheduled"}
                  </span>
                </div>

                <div className="text-[10px] text-zinc-500">
                  Event ID: {event.event_id}
                </div>
              </div>

              <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
                {awayName} @ {homeName}
              </h1>

              <p className="text-xs text-zinc-400">
                {event.date} · {event.venue || "TBD"}
              </p>
            </section>

            {/* Prediction panel */}
            <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 p-4 space-y-3">
              <div className="flex items-center justify-between">
                <h2 className="text-sm font-semibold text-zinc-100">
                  Model Prediction
                </h2>
                <div className="text-[10px] text-zinc-500 text-right space-y-0.5">
                  <div>
                    Model:{" "}
                    <span className="font-mono">
                      {prediction?.model_key ?? "nba-winprob-0.1.0"}
                    </span>
                  </div>
                  <div>
                    Generated: {prediction?.generated_at ?? generatedAt}
                  </div>
                </div>
              </div>

              {predLoading && (
                <p className="text-xs text-zinc-500">Loading prediction…</p>
              )}

              {predError && !predLoading && (
                <p className="text-xs text-red-400">{predError}</p>
              )}

              {prediction && !predLoading && !predError && (
                <div className="rounded-xl bg-zinc-900/60 border border-zinc-800 px-3 py-2 text-xs text-zinc-200 flex flex-col gap-2">
                  <div className="flex justify-between">
                    <span className="text-zinc-400">
                      {homeName || "Home"} win prob
                    </span>
                    <span className="font-medium">
                      {(
                        (prediction.win_probabilities.home ?? 0) * 100
                      ).toFixed(1)}
                      %
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-400">
                      {awayName || "Away"} win prob
                    </span>
                    <span className="font-medium">
                      {(
                        (prediction.win_probabilities.away ?? 0) * 100
                      ).toFixed(1)}
                      %
                    </span>
                  </div>
                </div>
              )}

              {!prediction && !predLoading && !predError && (
                <p className="text-[11px] text-zinc-500">
                  No prediction yet. Once the model is wired to this event, it
                  will appear here.
                </p>
              )}
            </section>

            {/* 🔹 Insights panel (now live) */}
            <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 p-4 space-y-3">
              <div className="flex items-center justify-between">
                <h2 className="text-sm font-semibold text-zinc-100">Insights</h2>
                {/* Could add model_key / generated_at here later if useful */}
              </div>

              {insightsLoading && (
                <p className="text-xs text-zinc-500">Loading insights…</p>
              )}

              {insightsError && !insightsLoading && (
                <p className="text-xs text-red-400">{insightsError}</p>
              )}

              {insights &&
                !insightsLoading &&
                !insightsError &&
                insights.length > 0 && (
                  <ul className="space-y-2 text-xs">
                    {insights.map((insight, idx) => (
                      <li key={idx} className="space-y-0.5">
                        <div className="flex justify-between gap-2">
                          <span className="font-medium text-zinc-100">
                            {insight.label}
                          </span>
                          {insight.value != null && (
                            <span className="text-[11px] text-zinc-400">
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
                )}

              {(!insights || insights?.length === 0) &&
                !insightsLoading &&
                !insightsError && (
                  <p className="text-xs text-zinc-400">
                    No insights available yet for this matchup.
                  </p>
                )}
            </section>

            {/* Event info panel */}
            <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 p-4 space-y-3">
              <h2 className="text-sm font-semibold text-zinc-100">
                Event Info
              </h2>
              <dl className="grid grid-cols-1 sm:grid-cols-2 gap-y-1 text-xs text-zinc-300">
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Sport</dt>
                  <dd>{sportLabelFromId(event.sport_id)}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Status</dt>
                  <dd>{event.status || "scheduled"}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Home</dt>
                  <dd>{homeName}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-zinc-500">Away</dt>
                  <dd>{awayName}</dd>
                </div>
              </dl>
            </section>

            {/* Raw JSON debug */}
            <section className="rounded-2xl border border-zinc-900 bg-zinc-950/80 p-4">
              <details className="text-xs text-zinc-400">
                <summary className="cursor-pointer mb-1">
                  Raw event JSON (debug)
                </summary>
                <pre className="mt-2 whitespace-pre-wrap break-all text-[10px]">
                  {JSON.stringify(event, null, 2)}
                </pre>
              </details>
            </section>
          </div>
        )}
      </div>
    </main>
  );
}