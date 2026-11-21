import { test } from 'node:test';
import assert from 'node:assert/strict';
import { OWNER, readOnlyReason, VIEWER_SQL } from './owner.ts';

const seen = (name: string | null) => ({ loading: false, error: false, name });

test('only the owner can act', () => {
  assert.equal(readOnlyReason('alice', seen('alice')), null);
  assert.equal(readOnlyReason('alice', seen('someone')), 'Read-only: actions are run by alice.');
});

test('an unknown viewer is read-only (fail closed)', () => {
  for (const viewer of [seen(null), seen(''), { loading: false, error: true, name: null }]) {
    assert.equal(readOnlyReason('alice', viewer), 'Read-only: actions are run by alice.');
  }
  assert.match(readOnlyReason('alice', { loading: true, error: false, name: null }) ?? '', /^Read-only until/);
});

test('a Dive deployed without an owner is read-only for everyone', () => {
  assert.match(readOnlyReason(null, seen('alice')) ?? '', /without an owner/);
  assert.match(readOnlyReason('', seen('')) ?? '', /without an owner/);
});

test('unbundled, the owner is unset and the viewer is current_user', () => {
  assert.equal(OWNER, null);
  assert.equal(VIEWER_SQL, 'SELECT current_user AS viewer');
});
