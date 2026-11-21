import { test } from 'node:test';
import assert from 'node:assert/strict';
import { formatAge, formatBytes, formatSeconds, formatTs, formatUntil, listOf, q } from './format.ts';

test('bytes use the binary scale', () => {
  assert.equal(formatBytes(0), '0 B');
  assert.equal(formatBytes(1536), '1.5 KB');
  assert.equal(formatBytes(3 * 1024 ** 3), '3 GB');
  assert.equal(formatBytes(null), '—');
});

test('durations and relative times', () => {
  assert.equal(formatSeconds(42), '42 s');
  assert.equal(formatSeconds(90), '1.5 min');
  assert.equal(formatSeconds(7200), '2 h');
  assert.equal(formatSeconds(3 * 86400), '3 d');
  assert.equal(formatUntil(3600), 'in 1 h');
  assert.equal(formatUntil(-172800 * 2), '4 d ago');
  assert.equal(formatSeconds(undefined), '—');
});

test('SQL literals escape quotes', () => {
  assert.equal(q("it's"), "'it''s'");
});

test('lists read alike locally and in the MotherDuck runtime', () => {
  assert.deepEqual(listOf(['a', 'b']), ['a', 'b']);
  assert.deepEqual(listOf({ values: ['a'] }), ['a']);
  assert.deepEqual(listOf(null), []);
  assert.deepEqual(listOf('x'), ['x']);
});

test('timestamps read YYYY-MM-DD HH:MM:SS TZ in the given zone', () => {
  const t = Date.UTC(2026, 10, 9, 22, 2, 10) / 1000;
  assert.equal(formatTs(t, 'UTC'), '2026-11-09 22:02:10 UTC');
  assert.equal(formatTs(t, 'America/Los_Angeles'), '2026-11-09 14:02:10 PST');
  assert.equal(formatTs(Date.UTC(2026, 10, 9, 0, 0, 5) / 1000, 'UTC'), '2026-11-09 00:00:05 UTC');
  assert.equal(formatTs(null), '—');
});

test('ages are coarse', () => {
  assert.equal(formatAge(56 * 60 + 20), '56 min');
  assert.equal(formatAge(2 * 3600 + 18 * 60), '2 h 18 min');
  assert.equal(formatAge(26 * 3600), '26 h');
  assert.equal(formatAge(76 * 3600), '3 d 4 h');
});
