import { NextResponse } from "next/server";
import path from "path";
import { promises as fs } from "fs";
import duckdb from "duckdb";

type OddsRecord = {
  sport: string;
  commence_time_utc: string;
  home_team: string;
  away_team: string;
  bookmaker: string;
  market: string;
  outcome_name: string;
  outcome_price: number;
  point?: number | null;
  last_update_utc: string;
  source: string;
};

type GameOdds = {
  home_team: string;
  away_team: string;
  commence_time_utc: string;
  moneyline: OddsRecord[];
  spreads: OddsRecord[];
};

const FILE_BY_SPORT: Record<string, string> = {
  nba: "nba_odds.parquet",
  nfl: "nfl_odds.parquet",
  nhl: "nhl_odds.parquet",
  ufc: "ufc_odds.parquet",
};

const HOURS_DEFAULT = 48;
const LOG_ENABLED = process.env.NODE_ENV !== "production";

function logDev(message: string, extra?: unknown) {
  if (!LOG_ENABLED) return;
  if (extra !== undefined) {
    console.log(message, extra);
  } else {
    console.log(message);
  }
}

function normalizeTeamName(raw: string | null | undefined): string {
  if (!raw) return "";
  const cleaned = raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  // Handle common shorthand (e.g., "la clippers" -> "los angeles clippers")
  const replacements: Record<string, string> = {
    "la clippers": "los angeles clippers",
    "la lakers": "los angeles lakers",
    "ny knicks": "new york knicks",
    "ny nets": "brooklyn nets",
  };

  return replacements[cleaned] ?? cleaned;
}

function parseDate(value: string | Date | null | undefined): number | null {
  if (!value) return null;
  if (value instanceof Date) {
    const t = value.getTime();
    return Number.isNaN(t) ? null : t;
  }
  const t = Date.parse(value);
  return Number.isNaN(t) ? null : t;
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  const sport = (url.searchParams.get("sport") ?? "nba").toLowerCase();
  const hoursParam = Number(url.searchParams.get("hours") ?? HOURS_DEFAULT);
  const startParam = url.searchParams.get("start");
  const endParam = url.searchParams.get("end");

  const now = new Date();
  const start = startParam ? new Date(startParam) : now;
  const end = endParam
    ? new Date(endParam)
    : new Date(start.getTime() + hoursParam * 60 * 60 * 1000);

  const filename = FILE_BY_SPORT[sport] ?? FILE_BY_SPORT.nba;
  const oddsPath = path.join(
    process.cwd(),
    "..",
    "model",
    "data",
    "processed",
    "odds",
    filename,
  );

  try {
    await fs.access(oddsPath);
  } catch {
    logDev(`Odds file not found: ${oddsPath}`);
    return NextResponse.json(
      {
        games: [],
        meta: {
          sport,
          file_found: false,
          rows_loaded: 0,
          rows_filtered: 0,
          start,
          end,
        },
        message: "No odds available",
      },
      { status: 200 },
    );
  }

  let rows: OddsRecord[] = [];
  try {
    const db = new duckdb.Database(":memory:");
    rows = await new Promise((resolve, reject) => {
      db.all(
        "SELECT * FROM read_parquet(?)",
        [oddsPath],
        (err: Error | null, res: any[]) => {
          if (err) return reject(err);
          resolve(res as OddsRecord[]);
        },
      );
    });
  } catch (err) {
    logDev("Failed to read parquet via DuckDB", err);
    return NextResponse.json(
      {
        games: [],
        meta: {
          sport,
          file_found: true,
          rows_loaded: 0,
          rows_filtered: 0,
          start,
          end,
        },
        message: "Failed to read odds file",
      },
      { status: 500 },
    );
  }

  const startMs = start.getTime();
  const endMs = end.getTime();

  const filtered = rows.filter((r) => {
    const ts = parseDate(r.commence_time_utc);
    if (ts === null) return false;
    return ts >= startMs && ts <= endMs;
  });

  const grouped = new Map<string, GameOdds>();
  for (const row of filtered) {
    const key = [
      normalizeTeamName(row.home_team),
      normalizeTeamName(row.away_team),
      row.commence_time_utc,
    ].join("|");

    const existing = grouped.get(key) ?? {
      home_team: row.home_team,
      away_team: row.away_team,
      commence_time_utc: row.commence_time_utc,
      moneyline: [] as OddsRecord[],
      spreads: [] as OddsRecord[],
    };

    if (row.market === "h2h") {
      existing.moneyline.push(row);
    } else if (row.market === "spreads") {
      existing.spreads.push(row);
    }

    grouped.set(key, existing);
  }

  const payload = {
    games: Array.from(grouped.values()),
    meta: {
      sport,
      file_found: true,
      rows_loaded: rows.length,
      rows_filtered: filtered.length,
      start,
      end,
    },
  };

  logDev("Odds API meta:", payload.meta);

  return NextResponse.json(payload, { status: 200 });
}
