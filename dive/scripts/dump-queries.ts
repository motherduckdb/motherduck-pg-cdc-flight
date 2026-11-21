/**
 * Render every query in src/queries.ts into queries.snapshot.json, for all sources and
 * for one source (SNAPSHOT_SOURCE), with the columns each returns.
 * tests/test_dive_queries.py runs exactly these strings against a real cdc_ops, so
 * Python CI needs no Node; src/snapshot.test.ts fails when the file is stale.
 */
import { writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { render } from '../src/snapshot.ts';

const out = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'queries.snapshot.json');
writeFileSync(out, render());
console.log(`wrote ${out}`);
