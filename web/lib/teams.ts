// web/lib/teams.ts
import type { Team, Event } from "./api";

export type TeamsById = Map<number, Team>;

/**
 * Build a Map from team_id -> Team for quick lookup.
 */
export function buildTeamsById(teams: Team[]): TeamsById {
  const map = new Map<number, Team>();
  for (const t of teams) {
    map.set(t.team_id, t);
  }
  return map;
}

/**
 * Given a TeamsById map and a team id, return a friendly label.
 */
export function teamLabelFromMap(
  teamsById: TeamsById,
  id: number | null
): string {
  if (id == null) return "TBD";
  const team = teamsById.get(id);
  if (!team) return `#${id}`;
  return team.name;
}

/**
 * Get team label for a game card, prioritizing event fields over lookups.
 *
 * Priority:
 * 1. Event's own team field (home_team/away_team) - works for all sports
 * 2. Lookup via teamsById[team_id] - fallback for older data
 * 3. #${id} - last resort if everything fails
 */
export function getTeamLabel(
  event: Event,
  side: "home" | "away",
  teamsById: TeamsById
): string {
  const teamField = side === "home" ? event.home_team : event.away_team;
  const teamId = side === "home" ? event.home_team_id : event.away_team_id;

  // Priority 1: Use the team name/abbrev from the event itself
  if (teamField && teamField.trim()) {
    return teamField;
  }

  // Priority 2: Lookup by ID
  if (teamId != null) {
    const team = teamsById.get(teamId);
    if (team) {
      return team.name;
    }
  }

  // Priority 3: Fallback
  return teamId != null ? `#${teamId}` : "TBD";
}