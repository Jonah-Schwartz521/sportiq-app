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
      return "🏟️";
  }
}