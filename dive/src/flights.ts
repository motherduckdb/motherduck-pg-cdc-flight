// The action Flights' ids per source, injected by scripts/deploy_dive.py --flights
// (a viewer can't look up the owner's Flights). Empty until deployed with them: every
// action button then says the source has no action Flights configured.
import type { FlightKind } from './actions.ts';

export type SourceFlights = Partial<Record<FlightKind, string>>;

const INJECTED: string = '__CDC_FLIGHTS__';
export const FLIGHTS: Record<string, SourceFlights> = INJECTED.startsWith('{') ? JSON.parse(INJECTED) : {};

export const flightFor = (source: string | null | undefined, kind: FlightKind): string | null =>
  (source && FLIGHTS[source]?.[kind]) || null;
