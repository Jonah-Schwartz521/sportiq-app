// web/lib/logos.ts
import React from "react";

// Simple config per team. You can expand this map over time.
// Key should match whatever `teamLabel()` returns on your Events page.
type TeamLogoConfig = {
  abbrev: string; // short code shown in the badge
  bg: string;     // Tailwind bg color
  text: string;   // Tailwind text color
};

const NBA_TEAM_LOGOS: Record<string, TeamLogoConfig> = {
  "Chicago Bulls": { abbrev: "CHI", bg: "bg-red-700", text: "text-white" },
  "Denver Nuggets": { abbrev: "DEN", bg: "bg-sky-700", text: "text-yellow-200" },
  "Los Angeles Lakers": { abbrev: "LAL", bg: "bg-yellow-500", text: "text-purple-900" },
  "Miami Heat": { abbrev: "MIA", bg: "bg-red-800", text: "text-yellow-100" },
  // 👉 Add more teams here as you go...
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
      bg: "bg-zinc-800",
      text: "text-zinc-100",
    };

  return (
    <div
      className={[
        "inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[11px]",
        "border-black/40 shadow-sm shadow-black/40",
        config.bg,
        config.text,
      ].join(" ")}
    >
      <span className="font-semibold">{config.abbrev}</span>
      {value && (
        <span
          className={
            variant === "odds"
              ? "text-[10px] opacity-80"
              : "text-[10px]"
          }
        >
          {value}
        </span>
      )}
    </div>
  );
}