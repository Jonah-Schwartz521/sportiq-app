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
  // Fallback config if we don't have a custom entry
  const config =
    NBA_TEAM_LOGOS[teamName] ?? {
      abbrev: teamName.slice(0, 3).toUpperCase(),
      bg: "bg-zinc-600",
      text: "text-zinc-100",
    };

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

      <span className="font-semibold tracking-wide text-zinc-50">
        {config.abbrev}
      </span>

      {value && (
        <span className="text-[10px] opacity-80">
          {value}
        </span>
      )}
    </div>
  );
}