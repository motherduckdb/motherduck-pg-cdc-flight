/*! Adapted from motherduckdb/md-migration-assessment (dive/src/format.ts, e4ae932), Copyright 2026 MotherDuck, Apache-2.0. Modified by MotherDuck, 2026; see dive/THIRD_PARTY.md. */
// Value helpers. DuckDB values arrive as JS numbers, strings, or (in the MotherDuck
// runtime) BigInt / decimal objects, so every numeric field goes through N() before it
// reaches JSX or Recharts.

export const N = (v: unknown): number => (v == null ? 0 : Number(v));
export const S = (v: unknown): string => (v == null ? '' : String(v));

// Binary scale (1 GB = 1024^3 bytes).
const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
export function formatBytes(v: unknown, digits = 1): string {
  if (v == null || Number.isNaN(Number(v))) return '—';
  let n = Math.abs(Number(v));
  let i = 0;
  while (n >= 1024 && i < BYTE_UNITS.length - 1) {
    n /= 1024;
    i++;
  }
  const s = i === 0 ? String(Math.round(n)) : n.toFixed(n >= 100 ? 0 : digits).replace(/\.0$/, '');
  return `${Number(v) < 0 ? '-' : ''}${s} ${BYTE_UNITS[i]}`;
}

export const formatInt = (v: unknown): string => (v == null ? '—' : Math.round(Number(v)).toLocaleString('en-US'));

// one decimal, but "1 s" / "24 h" rather than "1.0 s" / "24.0 h" on round values
const one = (n: number) => n.toFixed(1).replace(/\.0$/, '');
/** A duration in seconds ("—" when unknown); negative values read as "in the past". */
export function formatSeconds(v: unknown): string {
  if (v == null || Number.isNaN(Number(v))) return '—';
  const s = Math.abs(Number(v));
  const text = s < 60 ? `${Math.round(s)} s` : s < 3600 ? `${one(s / 60)} min` : s < 48 * 3600 ? `${one(s / 3600)} h` : `${one(s / 86400)} d`;
  return Number(v) < 0 ? `-${text}` : text;
}

/** "in 3.5 h" / "2 d ago" for a signed number of seconds until an event. */
export function formatUntil(v: unknown): string {
  if (v == null || Number.isNaN(Number(v))) return '—';
  const s = Number(v);
  return s >= 0 ? `in ${formatSeconds(s)}` : `${formatSeconds(-s)} ago`;
}

/** A LIST value as an array: a JS array locally, a DuckDB list object (`.values`) in
 * the MotherDuck runtime. */
export function listOf(v: unknown): unknown[] {
  if (v == null) return [];
  if (Array.isArray(v)) return v;
  const values = (v as { values?: unknown }).values;
  if (Array.isArray(values)) return values;
  if (typeof values === 'function') return Array.from((values as () => Iterable<unknown>).call(v));
  return [v];
}

/** Escape a string for a single-quoted SQL literal. */
export const q = (s: string): string => `'${s.replace(/'/g, "''")}'`;

/** The rows of a useSQLQuery result (undefined or a non-array while loading). */
export const rowsOf = <T = Record<string, unknown>>(data: unknown): T[] => (Array.isArray(data) ? (data as T[]) : []);

const pad = (n: number) => String(n).padStart(2, '0');
/** The viewer's short zone name ("PST", "GMT+1") for a moment, from the browser. */
function zoneName(d: Date, timeZone?: string): string {
  const part = new Intl.DateTimeFormat('en-US', { timeZone, timeZoneName: 'short' })
    .formatToParts(d)
    .find((p) => p.type === 'timeZoneName');
  return part?.value ?? '';
}
/**
 * A moment as `YYYY-MM-DD HH:MM:SS TZ` in the viewer's time zone. Queries return epoch
 * seconds (`*_at_s`); the browser formats them.
 * `timeZone` is for tests only.
 */
export function formatTs(epochS: unknown, timeZone?: string): string {
  if (epochS == null || epochS === '' || Number.isNaN(Number(epochS))) return '—';
  const d = new Date(Number(epochS) * 1000);
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-US', {
      timeZone, hourCycle: 'h23', year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    }).formatToParts(d).map((p) => [p.type, p.value]),
  );
  return `${parts.year}-${parts.month}-${parts.day} ${pad(Number(parts.hour))}:${parts.minute}:${parts.second} ${zoneName(d, timeZone)}`;
}

/** "56 min", "2 h 18 min", "3 d 4 h": a coarse age for details lines. */
export function formatAge(seconds: unknown): string {
  if (seconds == null || Number.isNaN(Number(seconds))) return '—';
  const s = Math.max(0, Math.round(Number(seconds)));
  if (s < 60) return `${s} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  if (h < 48) return m % 60 ? `${h} h ${m % 60} min` : `${h} h`;
  const d = Math.floor(h / 24);
  return h % 24 ? `${d} d ${h % 24} h` : `${d} d`;
}
