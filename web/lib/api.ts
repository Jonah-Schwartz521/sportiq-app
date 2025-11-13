const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000";

export type Team = {
  team_id: number;
  sport_id: number;
  name: string;
};

export type Event = {
  event_id: number;
  sport_id: number;
  date: string;
  home_team_id: number | null;
  away_team_id: number | null;
  venue: string | null;
  status: string | null;
  start_time?: string | null;
};

export type EventForPicker = {
  event_id: number; 
  sport_id: number; 
  date: string; 
  home_team_id: number | null; 
  away_team_id: number | null; 
};

export type PredictResponse = {
  model_key: string;
  win_probabilities: Record<string, number>;
  generated_at: string;
};

export type PredictionSummary = {
  event_id: number;
  model_key: string;
  home_wp: number;
  away_wp: number;
  created_at: string;
};

export type Insight = {
  type: string;
  label: string;
  detail: string;
  value?: number | null;
};

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    cache: "no-store", // ensures no stale cache in dev
  });

  if (!res.ok) {
    throw new Error(`API ${path} failed: ${res.status}`);
  }

  return res.json();
}

async function postJSON<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`API POST ${path} failed: ${res.status}`);
  }

  return res.json();
}

export const api = {
  health: () => getJSON<{ status: string }>("/health"),

  teams: () => getJSON<{ items: Team[] }>("/teams?limit=5"),

  events: () => getJSON<{ items: Event[] }>("/events?limit=5"),

  eventsForPicker: ()=>
    getJSON<{ items: Event[] }>("/events?limit=50"),

  predict: (sport: string, eventId: number) =>
    postJSON<PredictResponse>(`/predict/${sport}`, { event_id: eventId }),

  predictions: () =>
    getJSON<{ items: PredictionSummary[] }>("/predictions?limit=5"),

  insights: (sport: string, eventId: number) =>
    getJSON<{
      event_id: number;
      sport: string;
      model_key: string;
      generated_at: string;
      insights: Insight[];
    }>(`/insights/${sport}/${eventId}`),
};