// Runtime errors stay on screen until dismissed (user report, 2026-11-11: a stack trace
// flashed and vanished on reload). Render errors (the boundary) and uncaught errors and
// rejections (window listeners) are kept, newest first, also in sessionStorage where the
// browser allows it, so a reload still shows them; each has a Copy button.
import { Component, useEffect, useState, type ReactNode } from 'react';
import { caps, CARD, INK, MONO, MUTED, PAGE } from './theme';

export type Caught = { at: number; where: string; text: string };

const KEY = 'cdc-dive-errors';
const MAX = 5;
let caught: Caught[] = load();
const listeners = new Set<() => void>();

function load(): Caught[] {
  try {
    const raw = globalThis.sessionStorage?.getItem(KEY);
    return raw ? (JSON.parse(raw) as Caught[]).slice(0, MAX) : [];
  } catch {
    return [];
  }
}
function save() {
  try {
    globalThis.sessionStorage?.setItem(KEY, JSON.stringify(caught));
  } catch {
    // storage blocked: the panel still shows them for this page load
  }
}

/** The error's full text: message, then stack (and React's component stack, if any). */
export function errorText(e: unknown, extra?: string | null): string {
  const err = e instanceof Error ? e : null;
  const head = err ? `${err.name}: ${err.message}` : String(e);
  const stack = err?.stack && !err.stack.startsWith(head) ? `${head}\n${err.stack}` : err?.stack ?? head;
  return extra ? `${stack}\n\nComponent stack:${extra}` : stack;
}

export function report(e: unknown, where: string, extra?: string | null) {
  const text = errorText(e, extra);
  if (caught[0]?.text === text) return; // the same error again (a re-render)
  caught = [{ at: Date.now(), where, text }, ...caught].slice(0, MAX);
  save();
  listeners.forEach((l) => l());
}
export function clearErrors() {
  caught = [];
  save();
  listeners.forEach((l) => l());
}

/** Browser noise, not the Dive's: Chrome's benign ResizeObserver loop notices. */
export const BENIGN = /^ResizeObserver loop (completed with undelivered notifications|limit exceeded)/;

if (typeof window !== 'undefined') {
  window.addEventListener('error', (ev) => {
    if (BENIGN.test(String(ev.message ?? ''))) return;
    report(ev.error ?? ev.message, 'uncaught error');
  });
  window.addEventListener('unhandledrejection', (ev) => report(ev.reason, 'unhandled promise rejection'));
}

/** Catches a render error below it: the rest of the page stays, with the error shown. */
export class ErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  componentDidCatch(error: unknown, info: { componentStack?: string | null }) {
    report(error, 'render', info.componentStack);
  }
  render() {
    if (!this.state.failed) return this.props.children;
    return (
      <div role="alert" style={{ border: `2px solid ${INK}`, borderRadius: 14, padding: '18px 20px', background: PAGE, display: 'flex', flexDirection: 'column', gap: 10 }}>
        <p style={{ margin: 0, fontWeight: 600 }}>This view failed to render.</p>
        <p style={{ margin: 0, fontSize: 13 }}>The error is below, with a Copy button. Try again re-renders it.</p>
        <button type="button" onClick={() => this.setState({ failed: false })} style={{ alignSelf: 'flex-start', ...caps(12, 0.1), padding: '8px 14px', border: `2px solid ${INK}`, borderRadius: 999, background: CARD, color: INK }}>Try again</button>
      </div>
    );
  }
}

/** The kept errors, until dismissed. */
export function ErrorPanel() {
  const [, setTick] = useState(0);
  const [copied, setCopied] = useState<number | null>(null);
  useEffect(() => {
    const l = () => setTick((t) => t + 1);
    listeners.add(l);
    return () => void listeners.delete(l);
  }, []);
  if (!caught.length) return null;
  const copy = async (c: Caught, i: number) => {
    try {
      await navigator.clipboard.writeText(c.text);
      setCopied(i);
    } catch {
      setCopied(null); // clipboard blocked: the text box below is selectable
    }
  };
  return (
    <aside role="alert" aria-label="Errors" style={{ position: 'fixed', left: 16, right: 16, bottom: 16, zIndex: 50, maxHeight: '45vh', overflowY: 'auto', background: CARD, border: `2px solid ${INK}`, borderRadius: 14, boxShadow: `-6px 6px 0 ${INK}`, padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
        <strong style={{ ...caps(12, 0.12) }}>{caught.length === 1 ? 'An error occurred' : `${caught.length} errors occurred`}</strong>
        <button type="button" onClick={clearErrors} style={{ ...caps(11, 0.1), padding: '6px 12px', border: `2px solid ${INK}`, borderRadius: 999, background: CARD, color: INK }}>Dismiss</button>
      </div>
      {caught.map((c, i) => (
        <div key={`${c.at}-${i}`} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 12, color: MUTED }}>{new Date(c.at).toLocaleString()} · {c.where}</span>
            <button type="button" onClick={() => void copy(c, i)} style={{ ...caps(11, 0.1), padding: '4px 10px', border: `2px solid ${INK}`, borderRadius: 999, background: CARD, color: INK }}>{copied === i ? 'Copied' : 'Copy'}</button>
          </div>
          <textarea readOnly value={c.text} rows={Math.min(10, c.text.split('\n').length + 1)} onFocus={(e) => e.currentTarget.select()} style={{ fontFamily: MONO, fontSize: 11, width: '100%', boxSizing: 'border-box', border: `1.5px solid ${INK}`, borderRadius: 8, padding: 8, background: PAGE, color: INK, resize: 'vertical' }} />
        </div>
      ))}
    </aside>
  );
}
