import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { render } from './snapshot.ts';

test('queries.snapshot.json is current (npm run dump-queries)', () => {
  const path = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'queries.snapshot.json');
  assert.equal(readFileSync(path, 'utf-8'), render());
});
