// Who may act: only the Flights' owner (docs/dive.md §Actions). scripts/deploy_dive.py
// injects the owner's user name (--expect-user, the user it deploys as); the Dive's
// queries run as the viewer, so `SELECT current_user` names who is looking. Anyone else,
// or a viewer the Dive can't identify, sees every screen read-only (fail closed). The
// Flights themselves are runnable only by their owner (no RUN grants): this only keeps
// the buttons from offering what the run would refuse.

const INJECTED_OWNER: string = '__CDC_OWNER__';
export const OWNER: string | null = INJECTED_OWNER.startsWith('__') ? null : INJECTED_OWNER;

/** Not in queries.ts's registry: it reads no table. */
export const VIEWER_SQL = 'SELECT current_user AS viewer';

export type Viewer = { loading: boolean; error: boolean; name: string | null };

/** null when the viewer may act; otherwise why the Dive is read-only for them. */
export function readOnlyReason(owner: string | null, viewer: Viewer): string | null {
  if (!owner) return 'Read-only: this Dive was deployed without an owner, so no one can act from it.';
  if (viewer.name && viewer.name === owner) return null;
  if (viewer.loading) return `Read-only until the Dive knows who you are: actions are run by ${owner}.`;
  return `Read-only: actions are run by ${owner}.`;
}
