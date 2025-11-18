// web/lib/teams.ts
import type { Team } from "./api";

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