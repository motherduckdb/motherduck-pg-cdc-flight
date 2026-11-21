// The redesign's visual tokens (the 2026-11-10 design: cream page, ink borders, offset
// shadows, filled status pills). Inline styles only: the Dive runtime has no stylesheet
// besides the one <style> element in index.tsx.
import type { CSSProperties } from 'react';

export const PAGE = '#F4EFEA';
export const CARD = '#FFFFFF';
export const INK = '#383838';
export const MUTED = '#555555';
export const RULE = '#D6CDBE';
export const HOVER = '#ECE6DF';
export const DISABLED = '#A89F94';

export const SANS = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
export const LABEL = 'Cousine, ui-monospace, Menlo, monospace';
export const MONO = "'JetBrains Mono', ui-monospace, Menlo, monospace";

/** Filled status pills. Their order is the sort order: problems first. */
export const STATUS = {
  needs_action: { label: 'Needs action', bg: '#FF9A94' },
  stale: { label: 'Stale', bg: '#F9EE3E' },
  paused: { label: 'Paused', bg: '#F9EE3E' },
  rebuilding: { label: 'Rebuilding', bg: '#97D4FF' },
  not_enabled: { label: 'Not enabled', bg: '#E1D6CB' },
  queued: { label: 'Queued', bg: '#ECE6DF' },
  waiting: { label: 'Waiting', bg: '#ECE6DF' },
  ok: { label: 'OK', bg: '#50C7B7' },
} as const;
export type Status = keyof typeof STATUS;
export const STATUS_ORDER = Object.keys(STATUS) as Status[];

/** A source's own pill (its header): the worst of its tables, or its own problem. */
export const SOURCE_STATUS = {
  needs_action: { label: 'Needs action', bg: '#FF9A94' },
  warning: { label: 'Warning', bg: '#F9EE3E' },
  recovering: { label: 'Recovering', bg: '#97D4FF' },
  in_progress: { label: 'In progress', bg: '#97D4FF' },
  not_enabled: { label: 'Not enabled', bg: '#E1D6CB' },
  healthy: { label: 'Healthy', bg: '#50C7B7' },
} as const;
export type SourceStatus = keyof typeof SOURCE_STATUS;

/** Sync history marks (6 h each). */
export const TICK = {
  ok: { bg: '#16AA98', label: 'All runs ok' },
  some: { bg: '#FFDE00', border: '#BFA800', label: 'Some failed' },
  failed: { bg: '#E23F35', label: 'All failed' },
  none: { bg: '#E1D6CB', label: 'Nothing synced' },
} as const;
export type Tick = keyof typeof TICK;

/** WAL limit bar: green, yellow from 50 %, red from 80 %. */
export const walColor = (pct: number): string => (pct >= 80 ? '#E23F35' : pct >= 50 ? '#FFDE00' : '#16AA98');

export const card: CSSProperties = {
  background: CARD,
  border: `2px solid ${INK}`,
  borderRadius: 14,
  boxShadow: `-8px 8px 0 ${INK}`,
};

export const caps = (size = 11, spacing = 0.13): CSSProperties => ({
  fontFamily: LABEL,
  fontSize: size,
  letterSpacing: `${spacing}em`,
  textTransform: 'uppercase',
});

export const pill = (bg: string): CSSProperties => ({
  ...caps(11, 0.1),
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  padding: '3px 9px',
  borderRadius: 999,
  whiteSpace: 'nowrap',
  color: INK,
  background: bg,
  border: `1.5px solid ${INK}`,
});

export const toggle = (on: boolean): CSSProperties => ({
  fontFamily: LABEL,
  fontSize: 12,
  letterSpacing: '0.08em',
  padding: '6px 12px',
  border: `2px solid ${INK}`,
  borderRadius: 999,
  background: on ? INK : CARD,
  color: on ? CARD : INK,
});

export const navTab = (on: boolean): CSSProperties => ({
  ...caps(13, 0.1),
  padding: '9px 16px',
  border: `2px solid ${INK}`,
  borderRadius: 999,
  background: on ? INK : PAGE,
  color: on ? CARD : INK,
});

export const CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Cousine:wght@400;700&family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');
  .ops { font-family: ${SANS}; color: ${INK}; font-size: 14px; line-height: 1.45; background: ${PAGE}; min-height: 100vh; }
  .ops a { color: ${INK}; }
  .ops a:hover { color: #000; }
  .ops button { font: inherit; cursor: pointer; }
  .ops button:focus-visible, .ops a:focus-visible { outline: 3px solid #2BA5FF; outline-offset: 2px; }
  .ops .rowhover:hover { background: ${HOVER}; }
  .ops .linkish { background: none; border: none; padding: 0; color: ${INK}; text-decoration: underline; font-size: 13px; }
  .ops .skeleton { background: ${HOVER}; border-radius: 10px; height: 48px; animation: ops-pulse 1.4s ease-in-out infinite; }
  @keyframes ops-pulse { 0%, 100% { opacity: 1; } 50% { opacity: .55; } }
`;
