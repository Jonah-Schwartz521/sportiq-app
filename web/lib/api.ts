// web/lib/api.ts

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:8000";

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

// For places that just need a lightweight event (like pickers)
export type EventForPicker = {
  event_id: number;
  sport_id: number;
  date: string;
  home_team_id: number | null;
  away_team_id: number | null;
};

// This is the **shape your React code expects**
export type PredictResponse = {
  game_id: number;
  date: string;
  home_team: string;
  away_team: string;
  p_home: number;
  p_away: number;
};

// Raw response shape from the new /predict/{sport} endpoint
type PredictApiResponse = {
  model_key: string;
  win_probabilities: {
    home: number;
    away: number;
  };
  generated_at: string;
};

export type Insight = {
  type: string;
  label: string;
  detail: string;
  value?: number | null;
};

// --- prediction log types (admin recent predictions) ----------------

export type PredictionLogItem = {
  game_id: number;
  date: string; // "2015-10-29"
  home_team: string;
  away_team: string;
  p_home: number;
  p_away: number;
  created_at: string; // ISO timestamp
};

export type PredictionLogResponse = {
  items: PredictionLogItem[];
};

// -------------------------------------------------------------------

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    cache: "no-store",
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
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`API ${path} failed: ${res.status}`);
  }

  return res.json();
}

export const api = {
  // --- health ---
  health: () =>
    getJSON<{
      status: string;
      num_games: number;
      model_loaded: boolean;
    }>("/health"),

  // --- teams ---
  teams: () => getJSON<{ items: Team[] }>("/teams?limit=100"),

  // --- events (for games list, admin, etc.) ---
  events: () => getJSON<{ items: Event[] }>("/events?limit=50"),

  // used by PredictPanel and PredictionsPanel as a “picker” source
  eventsForPicker: () =>
    getJSON<{ items: EventForPicker[] }>("/events?limit=50"),

  // --- single event ---
  eventById: (eventId: number) => getJSON<Event>(`/events/${eventId}`),

  // --- predictions ---
  // Call FastAPI POST /predict/{sport} with {"event_id": <number>}
  // and map it into the flat PredictResponse shape your UI expects.
  predict: async (eventId: number): Promise<PredictResponse> => {
    const raw = await postJSON<PredictApiResponse>("/predict/nba", {
      // IMPORTANT: backend expects "event_id", not "game_id"
      event_id: eventId,
    });

    // We no longer get team names / date from this endpoint.
    // For now, fill what we can so existing UI keeps working.
    return {
      game_id: eventId,
      date: "", // you can optionally wire this from events later
      home_team: "", // same here – can be resolved from Event + Team data
      away_team: "",
      p_home: raw.win_probabilities.home,
      p_away: raw.win_probabilities.away,
    };
  },

  // recent-predictions endpoint for admin surface
  predictions: (limit: number = 20) =>
    getJSON<PredictionLogResponse>(`/predictions?limit=${limit}`),

  // --- insights ---
  // wired to GET /insights/{sport}/{event_id}
  insights: (eventId: number) =>
    getJSON<{
      game_id?: number;
      event_id?: number;
      model_key: string;
      generated_at: string;
      insights: Insight[];
    }>(`/insights/nba/${eventId}`),
};