export type SportKey = "nba" | "mlb" | "nfl" | "nhl" | "ufc";

export function sportLabelFromId(id: number | null): string {
  switch (id) {
    case 1:
      return "NBA";
    case 2:
      return "MLB";
    case 3:
      return "NFL";
    case 4:
      return "NHL";
    case 5:
      return "UFC";
    default:
      return "Unknown";
  }
}

export function sportIconFromId(id: number | null): string {
  switch (id) {
    case 1:
      return "🏀";
    case 2:
      return "⚾️";
    case 3:
      return "🏈";
    case 4:
      return "🏒";
    case 5:
      return "🥊";
    default:
      return "❓";
  }
}

export function sportKeyFromId(id: number | null): SportKey {
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

export function sportIdFromKey(key: SportKey): number {
  switch (key) {
    case "nba":
      return 1;
    case "mlb":
      return 2;
    case "nfl":
      return 3;
    case "nhl":
      return 4;
    case "ufc":
      return 5;
  }
}