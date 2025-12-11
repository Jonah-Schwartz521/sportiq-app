// web/lib/teams.ts
import type { Team, Event } from "./api";

export type TeamsById = Map<number, Team>;

/**
 * NHL team abbreviations → full names
 * Exported for use across the app (games page, team filters, etc.)
 */
export const NHL_TEAM_NAMES: Record<string, string> = {
  // Core 32 franchises
  ANA: "Anaheim Ducks",
  ARI: "Arizona Coyotes",
  BOS: "Boston Bruins",
  BUF: "Buffalo Sabres",
  CAR: "Carolina Hurricanes",
  CBJ: "Columbus Blue Jackets",
  CGY: "Calgary Flames",
  CHI: "Chicago Blackhawks",
  COL: "Colorado Avalanche",
  DAL: "Dallas Stars",
  DET: "Detroit Red Wings",
  EDM: "Edmonton Oilers",
  FLA: "Florida Panthers",
  LAK: "Los Angeles Kings",
  MIN: "Minnesota Wild",
  MTL: "Montreal Canadiens",
  NJD: "New Jersey Devils",
  NSH: "Nashville Predators",
  NYI: "New York Islanders",
  NYR: "New York Rangers",
  OTT: "Ottawa Senators",
  PHI: "Philadelphia Flyers",
  PIT: "Pittsburgh Penguins",
  SEA: "Seattle Kraken",
  SJS: "San Jose Sharks",
  STL: "St. Louis Blues",
  TBL: "Tampa Bay Lightning",
  TOR: "Toronto Maple Leafs",
  UTA: "Utah Hockey Club",
  VAN: "Vancouver Canucks",
  VGK: "Vegas Golden Knights",
  WPG: "Winnipeg Jets",
  WSH: "Washington Capitals",

  // Alternative codes / legacy variations
  "L.A": "Los Angeles Kings",
  LA: "Los Angeles Kings",
  "N.J": "New Jersey Devils",
  NJ: "New Jersey Devils",
  "S.J": "San Jose Sharks",
  SJ: "San Jose Sharks",
  "T.B": "Tampa Bay Lightning",
  TB: "Tampa Bay Lightning",
  ATL: "Atlanta Thrashers", // Legacy team
  PHX: "Phoenix Coyotes",   // Legacy ARI code
  WAS: "Washington Capitals", // Alternate for WSH
};

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
 * Get full team name from abbreviation based on sport.
 * Falls back to the original code if no mapping exists.
 *
 * @param sportId - Sport ID (1=NBA, 2=MLB, 3=NFL, 4=NHL, etc.)
 * @param code - Team abbreviation (e.g., "CGY", "VAN", "TOR")
 * @returns Full team name (e.g., "Calgary Flames") or original code
 */
export function getFullTeamName(sportId: number, code: string): string {
  const key = code.toUpperCase().trim();

  // NHL (sport_id === 4)
  if (sportId === 4) {
    return NHL_TEAM_NAMES[key] ?? code;
  }

  // For other sports, return the code as-is (or add more mappings here)
  return code;
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