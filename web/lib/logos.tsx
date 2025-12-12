// web/lib/logos.ts
import React from "react";

// Simple config per team. You can expand this map over time.
// Key should match whatever `teamLabel()` returns on your Events page.
type TeamLogoConfig = {
  abbrev: string; // short code shown in the badge
  bg: string;     // Tailwind bg color for the inner color dot
  text: string;   // Tailwind text color (kept for future use if needed)
};

const NBA_TEAM_LOGOS: Record<string, TeamLogoConfig> = {
  // Eastern Conference
  "Atlanta Hawks": { abbrev: "ATL", bg: "bg-red-700", text: "text-yellow-100" },
  "Boston Celtics": { abbrev: "BOS", bg: "bg-green-700", text: "text-white" },
  "Brooklyn Nets": { abbrev: "BKN", bg: "bg-zinc-900", text: "text-zinc-100" },
  "Charlotte Hornets": { abbrev: "CHA", bg: "bg-teal-700", text: "text-purple-100" },
  "Chicago Bulls": { abbrev: "CHI", bg: "bg-red-700", text: "text-white" },
  "Cleveland Cavaliers": { abbrev: "CLE", bg: "bg-red-900", text: "text-yellow-100" },
  "Detroit Pistons": { abbrev: "DET", bg: "bg-blue-900", text: "text-red-200" },
  "Indiana Pacers": { abbrev: "IND", bg: "bg-blue-900", text: "text-yellow-200" },
  "Miami Heat": { abbrev: "MIA", bg: "bg-red-800", text: "text-yellow-100" },
  "Milwaukee Bucks": { abbrev: "MIL", bg: "bg-emerald-900", text: "text-emerald-100" },
  "Philadelphia 76ers": { abbrev: "PHI", bg: "bg-blue-800", text: "text-red-100" },
  "Toronto Raptors": { abbrev: "TOR", bg: "bg-red-800", text: "text-white" },
  "Washington Wizards": { abbrev: "WAS", bg: "bg-blue-900", text: "text-red-100" },

  // Western Conference
  "Dallas Mavericks": { abbrev: "DAL", bg: "bg-sky-900", text: "text-sky-100" },
  "Denver Nuggets": { abbrev: "DEN", bg: "bg-slate-900", text: "text-yellow-200" },
  "Golden State Warriors": { abbrev: "GSW", bg: "bg-blue-900", text: "text-yellow-200" },
  "Houston Rockets": { abbrev: "HOU", bg: "bg-red-800", text: "text-white" },
  "Los Angeles Clippers": { abbrev: "LAC", bg: "bg-blue-900", text: "text-red-100" },
  "Los Angeles Lakers": { abbrev: "LAL", bg: "bg-yellow-500", text: "text-purple-900" },
  "Memphis Grizzlies": { abbrev: "MEM", bg: "bg-slate-800", text: "text-sky-100" },
  "Minnesota Timberwolves": { abbrev: "MIN", bg: "bg-slate-900", text: "text-emerald-200" },
  "New Orleans Pelicans": { abbrev: "NOP", bg: "bg-slate-900", text: "text-yellow-200" },
  "Oklahoma City Thunder": { abbrev: "OKC", bg: "bg-sky-800", text: "text-orange-200" },
  "Phoenix Suns": { abbrev: "PHX", bg: "bg-purple-800", text: "text-orange-200" },
  "Portland Trail Blazers": { abbrev: "POR", bg: "bg-zinc-900", text: "text-red-200" },
  "Sacramento Kings": { abbrev: "SAC", bg: "bg-purple-800", text: "text-zinc-100" },
  "San Antonio Spurs": { abbrev: "SAS", bg: "bg-zinc-700", text: "text-zinc-100" },
  "Utah Jazz": { abbrev: "UTA", bg: "bg-purple-900", text: "text-yellow-100" },

  // Knicks at bottom for readability
  "New York Knicks": { abbrev: "NYK", bg: "bg-blue-800", text: "text-orange-200" },
};

const MLB_TEAM_LOGOS: Record<string, TeamLogoConfig> = {
  // American League
  "Baltimore Orioles":      { abbrev: "BAL", bg: "bg-orange-700", text: "text-zinc-100" },
  "Boston Red Sox":         { abbrev: "BOS", bg: "bg-red-800",    text: "text-zinc-100" },
  "New York Yankees":       { abbrev: "NYY", bg: "bg-navy-900",   text: "text-slate-100" },
  "Tampa Bay Rays":         { abbrev: "TBR", bg: "bg-sky-800",    text: "text-slate-100" },
  "Toronto Blue Jays":      { abbrev: "TOR", bg: "bg-sky-900",    text: "text-slate-50" },

  "Chicago White Sox":      { abbrev: "CWS", bg: "bg-zinc-900",   text: "text-zinc-100" },
  "Cleveland Guardians":    { abbrev: "CLE", bg: "bg-red-900",    text: "text-zinc-100" },
  "Detroit Tigers":         { abbrev: "DET", bg: "bg-navy-900",   text: "text-amber-200" },
  "Kansas City Royals":     { abbrev: "KCR", bg: "bg-sky-800",    text: "text-slate-50" },
  "Minnesota Twins":        { abbrev: "MIN", bg: "bg-navy-900",   text: "text-red-200" },

  "Houston Astros":         { abbrev: "HOU", bg: "bg-navy-900",   text: "text-orange-300" },
  "Los Angeles Angels":     { abbrev: "LAA", bg: "bg-red-800",    text: "text-slate-50" },
  "Oakland Athletics":      { abbrev: "OAK", bg: "bg-emerald-900",text: "text-amber-200" },
  "Seattle Mariners":       { abbrev: "SEA", bg: "bg-teal-900",   text: "text-slate-50" },
  "Texas Rangers":          { abbrev: "TEX", bg: "bg-blue-900",   text: "text-red-200" },

  // National League
  "Arizona Diamondbacks":   { abbrev: "ARI", bg: "bg-red-900",    text: "text-slate-50" },
  "Colorado Rockies":       { abbrev: "COL", bg: "bg-purple-900", text: "text-slate-50" },
  "Los Angeles Dodgers":    { abbrev: "LAD", bg: "bg-blue-900",   text: "text-slate-50" },
  "San Diego Padres":       { abbrev: "SDP", bg: "bg-amber-900",  text: "text-stone-100" },
  "San Francisco Giants":   { abbrev: "SFG", bg: "bg-zinc-900",   text: "text-orange-300" },

  "Chicago Cubs":           { abbrev: "CHC", bg: "bg-blue-900",   text: "text-red-200" },
  "Cincinnati Reds":        { abbrev: "CIN", bg: "bg-red-800",    text: "text-slate-50" },
  "Milwaukee Brewers":      { abbrev: "MIL", bg: "bg-navy-900",   text: "text-amber-200" },
  "Pittsburgh Pirates":     { abbrev: "PIT", bg: "bg-zinc-900",   text: "text-yellow-300" },
  "St. Louis Cardinals":    { abbrev: "STL", bg: "bg-red-800",    text: "text-slate-50" },

  "Atlanta Braves":         { abbrev: "ATL", bg: "bg-red-800",    text: "text-navy-100" },
  "Miami Marlins":          { abbrev: "MIA", bg: "bg-teal-800",   text: "text-slate-50" },
  "New York Mets":          { abbrev: "NYM", bg: "bg-blue-900",   text: "text-orange-300" },
  "Philadelphia Phillies":  { abbrev: "PHI", bg: "bg-red-900",    text: "text-slate-50" },
  "Washington Nationals":   { abbrev: "WSN", bg: "bg-red-900",    text: "text-navy-100" },
};

const NFL_TEAM_LOGOS: Record<string, TeamLogoConfig> = {
  // AFC
  "Baltimore Ravens":       { abbrev: "BAL", bg: "bg-purple-900", text: "text-zinc-100" },
  "Buffalo Bills":          { abbrev: "BUF", bg: "bg-blue-900",   text: "text-red-200" },
  "Cincinnati Bengals":     { abbrev: "CIN", bg: "bg-orange-700", text: "text-zinc-900" },
  "Cleveland Browns":       { abbrev: "CLE", bg: "bg-orange-800", text: "text-zinc-900" },
  "Denver Broncos":         { abbrev: "DEN", bg: "bg-blue-900",   text: "text-orange-300" },
  "Houston Texans":         { abbrev: "HOU", bg: "bg-navy-900",   text: "text-red-200" },
  "Indianapolis Colts":     { abbrev: "IND", bg: "bg-blue-900",   text: "text-slate-50" },
  "Jacksonville Jaguars":   { abbrev: "JAX", bg: "bg-teal-800",   text: "text-yellow-200" },
  "Kansas City Chiefs":     { abbrev: "KC",  bg: "bg-red-800",    text: "text-yellow-200" },
  "Las Vegas Raiders":      { abbrev: "LV",  bg: "bg-zinc-900",   text: "text-slate-100" },
  "Los Angeles Chargers":   { abbrev: "LAC", bg: "bg-sky-800",    text: "text-yellow-200" },
  "Miami Dolphins":         { abbrev: "MIA", bg: "bg-teal-700",   text: "text-orange-200" },
  "New England Patriots":   { abbrev: "NE",  bg: "bg-navy-900",   text: "text-red-200" },
  "New York Jets":          { abbrev: "NYJ", bg: "bg-emerald-900",text: "text-slate-50" },
  "Pittsburgh Steelers":    { abbrev: "PIT", bg: "bg-zinc-900",   text: "text-yellow-300" },
  "Tennessee Titans":       { abbrev: "TEN", bg: "bg-sky-900",    text: "text-red-200" },

  // NFC
  "Arizona Cardinals":      { abbrev: "ARI", bg: "bg-red-800",    text: "text-zinc-100" },
  "Atlanta Falcons":        { abbrev: "ATL", bg: "bg-red-900",    text: "text-zinc-100" },
  "Carolina Panthers":      { abbrev: "CAR", bg: "bg-cyan-900",   text: "text-slate-50" },
  "Chicago Bears":          { abbrev: "CHI", bg: "bg-navy-900",   text: "text-orange-200" },
  "Dallas Cowboys":         { abbrev: "DAL", bg: "bg-slate-900",  text: "text-slate-50" },
  "Detroit Lions":          { abbrev: "DET", bg: "bg-sky-800",    text: "text-slate-50" },
  "Green Bay Packers":      { abbrev: "GB",  bg: "bg-green-900",  text: "text-yellow-200" },
  "Los Angeles Rams":       { abbrev: "LAR", bg: "bg-blue-900",   text: "text-yellow-200" },
  "Minnesota Vikings":      { abbrev: "MIN", bg: "bg-purple-900", text: "text-yellow-200" },
  "New Orleans Saints":     { abbrev: "NO",  bg: "bg-zinc-900",   text: "text-amber-200" },
  "New York Giants":        { abbrev: "NYG", bg: "bg-blue-900",   text: "text-red-200" },
  "Philadelphia Eagles":    { abbrev: "PHI", bg: "bg-emerald-900",text: "text-slate-50" },
  "San Francisco 49ers":    { abbrev: "SF",  bg: "bg-red-900",    text: "text-yellow-200" },
  "Seattle Seahawks":       { abbrev: "SEA", bg: "bg-teal-900",   text: "text-lime-200" },
  "Tampa Bay Buccaneers":   { abbrev: "TB",  bg: "bg-red-900",    text: "text-slate-50" },
  "Washington Commanders":  { abbrev: "WAS", bg: "bg-red-900",    text: "text-amber-200" },
};

const NHL_TEAM_LOGOS: Record<string, TeamLogoConfig> = {
  // Metropolitan Division
  "Carolina Hurricanes":    { abbrev: "CAR", bg: "bg-red-800",    text: "text-zinc-100" },
  "Columbus Blue Jackets":  { abbrev: "CBJ", bg: "bg-blue-900",   text: "text-red-200" },
  "New Jersey Devils":      { abbrev: "NJD", bg: "bg-red-800",    text: "text-zinc-100" },
  "New York Islanders":     { abbrev: "NYI", bg: "bg-blue-900",   text: "text-orange-200" },
  "New York Rangers":       { abbrev: "NYR", bg: "bg-blue-900",   text: "text-red-200" },
  "Philadelphia Flyers":    { abbrev: "PHI", bg: "bg-orange-700", text: "text-zinc-100" },
  "Pittsburgh Penguins":    { abbrev: "PIT", bg: "bg-zinc-900",   text: "text-yellow-300" },
  "Washington Capitals":    { abbrev: "WSH", bg: "bg-red-800",    text: "text-blue-200" },

  // Atlantic Division
  "Boston Bruins":          { abbrev: "BOS", bg: "bg-yellow-500", text: "text-zinc-900" },
  "Buffalo Sabres":         { abbrev: "BUF", bg: "bg-blue-900",   text: "text-yellow-200" },
  "Detroit Red Wings":      { abbrev: "DET", bg: "bg-red-800",    text: "text-zinc-100" },
  "Florida Panthers":       { abbrev: "FLA", bg: "bg-red-800",    text: "text-blue-200" },
  "Montreal Canadiens":     { abbrev: "MTL", bg: "bg-red-800",    text: "text-blue-200" },
  "Ottawa Senators":        { abbrev: "OTT", bg: "bg-red-800",    text: "text-zinc-100" },
  "Tampa Bay Lightning":    { abbrev: "TBL", bg: "bg-blue-900",   text: "text-zinc-100" },
  "Toronto Maple Leafs":    { abbrev: "TOR", bg: "bg-blue-900",   text: "text-zinc-100" },

  // Central Division
  "Arizona Coyotes":        { abbrev: "ARI", bg: "bg-red-900",    text: "text-zinc-100" },
  "Chicago Blackhawks":     { abbrev: "CHI", bg: "bg-red-800",    text: "text-zinc-100" },
  "Colorado Avalanche":     { abbrev: "COL", bg: "bg-red-900",    text: "text-blue-200" },
  "Dallas Stars":           { abbrev: "DAL", bg: "bg-green-700",  text: "text-zinc-100" },
  "Minnesota Wild":         { abbrev: "MIN", bg: "bg-green-800",  text: "text-red-200" },
  "Nashville Predators":    { abbrev: "NSH", bg: "bg-yellow-600", text: "text-blue-900" },
  "St. Louis Blues":        { abbrev: "STL", bg: "bg-blue-900",   text: "text-yellow-200" },
  "Utah Hockey Club":       { abbrev: "UTA", bg: "bg-blue-900",   text: "text-zinc-100" },
  "Winnipeg Jets":          { abbrev: "WPG", bg: "bg-blue-900",   text: "text-zinc-100" },

  // Pacific Division
  "Anaheim Ducks":          { abbrev: "ANA", bg: "bg-orange-700", text: "text-zinc-100" },
  "Calgary Flames":         { abbrev: "CGY", bg: "bg-red-800",    text: "text-yellow-200" },
  "Edmonton Oilers":        { abbrev: "EDM", bg: "bg-blue-900",   text: "text-orange-300" },
  "Los Angeles Kings":      { abbrev: "LAK", bg: "bg-zinc-900",   text: "text-zinc-100" },
  "San Jose Sharks":        { abbrev: "SJS", bg: "bg-teal-700",   text: "text-zinc-100" },
  "Seattle Kraken":         { abbrev: "SEA", bg: "bg-teal-900",   text: "text-zinc-100" },
  "Vancouver Canucks":      { abbrev: "VAN", bg: "bg-blue-900",   text: "text-green-300" },
  "Vegas Golden Knights":   { abbrev: "VGK", bg: "bg-yellow-700", text: "text-zinc-900" },

  // Historic teams
  "Atlanta Thrashers":      { abbrev: "ATL", bg: "bg-blue-900",   text: "text-red-200" },
};

// Props for the badge that can show either a score or an odds value.
type TeamValueBadgeProps = {
  teamName: string;
  value: string | null;         // score or odds text
  variant?: "score" | "odds";   // just for subtle styling differences
};

export function TeamValueBadge({
  teamName,
  value,
  variant = "score",
}: TeamValueBadgeProps) {
  const resolveTeam = (name: string): { config: TeamLogoConfig; displayName: string } => {
    // Check by full name keys first
    if (NBA_TEAM_LOGOS[name]) {
      return { config: NBA_TEAM_LOGOS[name], displayName: name };
    }
    if (MLB_TEAM_LOGOS[name]) {
      return { config: MLB_TEAM_LOGOS[name], displayName: name };
    }
    if (NFL_TEAM_LOGOS[name]) {
      return { config: NFL_TEAM_LOGOS[name], displayName: name };
    }
    if (NHL_TEAM_LOGOS[name]) {
      return { config: NHL_TEAM_LOGOS[name], displayName: name };
    }

    // Search by abbreviation in NBA_TEAM_LOGOS
    for (const [fullName, cfg] of Object.entries(NBA_TEAM_LOGOS)) {
      if (cfg.abbrev === name) {
        return { config: cfg, displayName: fullName };
      }
    }
    // Search by abbreviation in MLB_TEAM_LOGOS
    for (const [fullName, cfg] of Object.entries(MLB_TEAM_LOGOS)) {
      if (cfg.abbrev === name) {
        return { config: cfg, displayName: fullName };
      }
    }
    // Search by abbreviation in NFL_TEAM_LOGOS
    for (const [fullName, cfg] of Object.entries(NFL_TEAM_LOGOS)) {
      if (cfg.abbrev === name) {
        return { config: cfg, displayName: fullName };
      }
    }
    // Search by abbreviation in NHL_TEAM_LOGOS
    for (const [fullName, cfg] of Object.entries(NHL_TEAM_LOGOS)) {
      if (cfg.abbrev === name) {
        return { config: cfg, displayName: fullName };
      }
    }

    // Fallback
    return {
      config: {
        abbrev: name.slice(0, 3).toUpperCase(),
        bg: "bg-zinc-600",
        text: "text-zinc-100",
      },
      displayName: name,
    };
  };

  const { config, displayName } = resolveTeam(teamName);

  return (
    <div
      className={[
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px]",
        "border-zinc-700/80 bg-zinc-950/90 text-zinc-100 shadow-sm shadow-black/40",
        variant === "odds" ? "opacity-80" : "opacity-100",
      ].join(" ")}
    >
      {/* Tiny ring + inner color dot for the team */}
      <span className="relative inline-flex h-3 w-3 items-center justify-center rounded-full bg-zinc-900 shadow-inner shadow-black/60">
        <span className={["h-2 w-2 rounded-full", config.bg].join(" ")} />
      </span>

      <span className="font-semibold tracking-wide text-zinc-50 max-w-[120px] truncate">
        {displayName}
      </span>

      {value && (
        <span className="text-[10px] opacity-80">
          {value}
        </span>
      )}
    </div>
  );
}