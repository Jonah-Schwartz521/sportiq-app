"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { api, type Event, type PredictResponse, type Insight } from "@/lib/api";

type SportKey = "nba" | "mlb" | "nfl" | "nhl" | "ufc";

// ⚙️ simple hard-coded mapping for now
function sportKeyFromId(id: number | null): SportKey | null {
  if (id === null || id === undefined) return null;

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
      return null;
  }
}

type InsightsResponse = {
  event_id: number;
  sport: string;
  model_key: string;
  generated_at: string;
  insights: Insight[];
};

export default function GameDetailPage() {
  const params = useParams<{ eventId: string }>();
  const router = useRouter();
  const eventIdNum = Number(params.eventId);

  const [event, setEvent] = useState<Event | null>(null);
  const [eventLoading, setEventLoading] = useState(true);
  const [eventError, setEventError] = useState<string | null>(null);

  const [prediction, setPrediction] = useState<PredictResponse | null>(null);
  const [predictionLoading, setPredictionLoading] = useState(false);
  const [predictionError, setPredictionError] = useState<string | null>(null);

  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);

  // Redirect if eventId is bogus
  useEffect(() => {
    if (!eventIdNum || Number.isNaN(eventIdNum)) {
      router.push("/games");
    }
  }, [eventIdNum, router]);

  // Load event basics
  useEffect(() => {
    if (!eventIdNum || Number.isNaN(eventIdNum)) return;

    (async () => {
      try {
        setEventLoading(true);
        const data = await api.eventById(eventIdNum);
        setEvent(data);
      } catch (err: unknown) {
        console.error(err);
        const msg =
          err instanceof Error ? err.message : "Failed to load event";
        setEventError(msg);
      } finally {
        setEventLoading(false);
      }
    })();
  }, [eventIdNum]);

  // Once event is loaded, fetch prediction + insights if supported
  useEffect(() => {
    if (!event || !event.event_id) return;

    const sportKey = sportKeyFromId(event.sport_id);
    if (!sportKey) {
      // sport not wired yet, nothing to fetch
      return;
    }

    // prediction
    (async () => {
      try {
        setPredictionLoading(true);
        setPredictionError(null);
        const data = await api.predict(sportKey, event.event_id);
        setPrediction(data);
      } catch (err: unknown) {
        console.error(err);
        const msg =
          err instanceof Error ? err.message : "Failed to fetch prediction";
        setPredictionError(msg);
      } finally {
        setPredictionLoading(false);
      }
    })();

    // insights (only for nba + ufc with your current backend)
    if (sportKey === "nba" || sportKey === "ufc") {
      (async () => {
        try {
          setInsightsLoading(true);
          setInsightsError(null);
          const data = await api.insights(sportKey, event.event_id);
          setInsights(data);
        } catch (err: unknown) {
          console.error(err);
          const msg =
            err instanceof Error ? err.message : "Failed to fetch insights";
          setInsightsError(msg);
        } finally {
          setInsightsLoading(false);
        }
      })();
    }
  }, [event]);

  return (
    <main className="min-h-screen bg-black text-white flex justify-center px-4 py-10">
      <div className="w-full max-w-3xl space-y-6">
        {/* Back link */}
        <button
          onClick={() => router.push("/games")}
          className="text-xs text-zinc-400 hover:text-zinc-200"
        >
          ← Back to games
        </button>

        {/* Event header */}
        <header className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">
            Game #{eventIdNum}
          </h1>
          {event && (
            <p className="text-xs text-zinc-500">
              {event.date} · {event.venue || "TBD"}
            </p>
          )}
        </header>

        {/* Event info */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4">
          {eventLoading ? (
            <p className="text-sm text-zinc-500">Loading event…</p>
          ) : eventError ? (
            <p className="text-sm text-red-400">{eventError}</p>
          ) : !event ? (
            <p className="text-sm text-zinc-500">Event not found.</p>
          ) : (
            <div className="text-sm text-zinc-200 space-y-2">
              <div className="flex flex-wrap gap-4">
                <div>
                  <div className="text-[10px] text-zinc-500 uppercase">
                    Status
                  </div>
                  <div>{event.status || "scheduled"}</div>
                </div>
                <div>
                  <div className="text-[10px] text-zinc-500 uppercase">
                    Home team
                  </div>
                  <div>#{event.home_team_id ?? "-"}</div>
                </div>
                <div>
                  <div className="text-[10px] text-zinc-500 uppercase">
                    Away team
                  </div>
                  <div>#{event.away_team_id ?? "-"}</div>
                </div>
              </div>

              <details className="mt-2">
                <summary className="cursor-pointer text-[10px] text-zinc-500">
                  Show raw event JSON
                </summary>
                <pre className="mt-1 bg-zinc-900/80 p-3 rounded text-[11px] overflow-x-auto">
                  {JSON.stringify(event, null, 2)}
                </pre>
              </details>
            </div>
          )}
        </section>

        {/* Prediction */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4 space-y-2">
          <div className="flex items-center justify-between mb-1">
            <h2 className="text-sm font-medium text-zinc-200">
              Win Probability
            </h2>
            <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
              POST /predict
            </span>
          </div>

          {predictionLoading ? (
            <p className="text-xs text-zinc-500">Loading prediction…</p>
          ) : predictionError ? (
            <p className="text-xs text-red-400">{predictionError}</p>
          ) : !prediction ? (
            <p className="text-xs text-zinc-500">
              No prediction available for this sport yet.
            </p>
          ) : (
            <>
              <div className="text-sm">
                Home{" "}
                <span className="font-mono">
                  {(prediction.win_probabilities.home * 100).toFixed(1)}%
                </span>{" "}
                · Away{" "}
                <span className="font-mono">
                  {(prediction.win_probabilities.away * 100).toFixed(1)}%
                </span>
              </div>
              <pre className="text-[11px] bg-zinc-900/80 p-3 rounded overflow-x-auto">
                {JSON.stringify(prediction, null, 2)}
              </pre>
            </>
          )}
        </section>

        {/* Insights */}
        <section className="rounded-2xl border border-zinc-800 bg-zinc-950/60 px-5 py-4 space-y-2">
          <div className="flex items-center justify-between mb-1">
            <h2 className="text-sm font-medium text-zinc-200">Insights</h2>
            <span className="text-[10px] text-zinc-500 uppercase tracking-[0.16em]">
              GET /insights
            </span>
          </div>

          {insightsLoading ? (
            <p className="text-xs text-zinc-500">Loading insights…</p>
          ) : insightsError ? (
            <p className="text-xs text-red-400">{insightsError}</p>
          ) : !insights ? (
            <p className="text-xs text-zinc-500">
              Insights not available for this sport yet.
            </p>
          ) : (
            <ul className="space-y-2">
              {insights.insights.map((ins, idx) => (
                <li
                  key={`${ins.type}-${ins.label}-${idx}`}
                  className="rounded-lg border border-zinc-800 bg-zinc-950/80 px-3 py-2"
                >
                  <div className="text-xs font-medium text-zinc-200">
                    {ins.label}
                  </div>
                  <p className="text-[11px] text-zinc-400">{ins.detail}</p>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </main>
  );
}