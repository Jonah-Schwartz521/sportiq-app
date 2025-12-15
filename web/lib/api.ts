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
  home_team?: string | null;  // Team name/abbrev from API
  away_team?: string | null;  // Team name/abbrev from API
  venue: string | null;
  status: string | null;
  start_time?: string | null;
  home_score?: number | null;
  away_score?: number | null;
  home_win?: boolean | null;
  model_home_win_prob?: number | null;
  model_away_win_prob?: number | null;
  model_home_american_odds?: number | null;
  model_away_american_odds?: number | null;
  sportsbook_home_american_odds?: number | null;
  sportsbook_away_american_odds?: number | null;
  model_snapshot?: {
    source: string;
    p_home_win: number;
    p_away_win: number;
  } | null;
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

export type Insight = {
  type: string;
  label: string;
  detail: string;
  value?: number | null;
};

export type OddsRecord = {
  sport: string;
  commence_time_utc: string;
  home_team: string;
  away_team: string;
  bookmaker: string;
  market: string; // 'h2h' or 'spreads'
  outcome_name: string;
  outcome_price: number; // American odds
  point?: number | null; // Spread value (null for moneyline)
  last_update_utc: string;
  source: string;
};

export type OddsForEvent = {
  event_id?: number | null;
  home_team: string;
  away_team: string;
  commence_time_utc: string;
  moneyline: OddsRecord[];
  spreads: OddsRecord[];
};

export type SeasonsMeta = {
  seasons: Record<string, number[]>;
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

export type Metrics = {
  num_games: number; 
  accuracy: number; 
  brier_score: number;
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

function buildQueryString(params?: Record<string, string | number | undefined>): string {
  if (!params) return "";
  const search = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue;
    search.set(key, String(value));
  }

  const qs = search.toString();
  return qs ? `?${qs}` : "";
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
  events: (opts?: { limit?: number; sport_id?: number; sport?: string; season?: string | number; date?: string }) => {
    const query = buildQueryString({
      // if caller doesn't pass a limit, pull a big slice so we get all seasons
      limit: opts?.limit ?? 50000,
      sport_id: opts?.sport_id,
      sport: opts?.sport,
      // send season through directly (string) and also send year when numeric for back-compat
      season: opts?.season === undefined ? undefined : String(opts.season),
      year:
        typeof opts?.season === "number"
          ? opts.season
          : typeof opts?.season === "string" && /^\d{4}$/.test(opts.season)
            ? Number(opts.season)
            : undefined,
      date: opts?.date,
    });

    return getJSON<{ items: Event[] }>(`/events${query}`);
  },

  // used by PredictPanel and PredictionsPanel as a “picker” source
  eventsForPicker: (opts?: { limit?: number; sport_id?: number; season?: number }) => {
    const query = buildQueryString({
      // keep a small default limit for picker dropdowns
      limit: opts?.limit ?? 50,
      sport_id: opts?.sport_id,
      // again, translate `season` to backend `year`
      year: opts?.season,
    });

    return getJSON<{ items: EventForPicker[] }>(`/events${query}`);
  },

  // --- single event ---
  eventById: (eventId: number) => getJSON<Event>(`/events/${eventId}`),

  // --- predictions ---
  // Call FastAPI GET /predict_by_game_id?game_id=<number>
  predict: (eventId: number) =>
    getJSON<PredictResponse>(`/predict_by_game_id?game_id=${eventId}`),

  // recent-predictions endpoint for admin surface
  predictions: (limit: number = 20) =>
    getJSON<PredictionLogResponse>(`/predictions?limit=${limit}`),

  // --- metrics ---
  metrics: () => getJSON<Metrics>("/metrics"),

  // --- insights ---
  // wired to GET /insights/{event_id}
  insights: (eventId: number) =>
    getJSON<{
      game_id?: number;
      event_id?: number;
      model_key: string;
      generated_at: string;
      insights: Insight[];
    }>(`/insights/${eventId}`),

  // --- odds ---
  // Get odds for a specific matchup
  oddsForEvent: (homeTeam: string, awayTeam: string, sport?: string) => {
    const query = buildQueryString({
      home_team: homeTeam,
      away_team: awayTeam,
      sport: sport,
    });
    return getJSON<OddsForEvent>(`/odds/event${query}`);
  },

  // --- seasons meta ---
  seasonsMeta: () => getJSON<SeasonsMeta>("/meta/seasons"),
};
