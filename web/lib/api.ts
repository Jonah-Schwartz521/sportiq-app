const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000";

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    // ensures no stale cache in dev
    cache: "no-store",
  });

  if (!res.ok) {
    throw new Error(`API ${path} failed: ${res.status}`);
  }

  return res.json();
}

export const api = {
  health: () => getJSON<{ status: string }>("/health"),
  teams: () =>
    getJSON<{ items: { team_id: number; sport_id: number; name: string }[] }>(
      "/teams?limit=5"
    ),
  events: () =>
    getJSON<{
      items: {
        event_id: number;
        sport_id: number;
        date: string;
        home_team_id: number | null;
        away_team_id: number | null;
        venue: string | null;
        status: string | null;
      }[];
    }>("/events?limit=5"),
};