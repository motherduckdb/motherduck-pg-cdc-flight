import { test } from 'node:test';
import assert from 'node:assert/strict';
import { clearErrors, errorText, report } from './errors.tsx';

test('an error keeps its message, stack and component stack; repeats are kept once', () => {
  const e = new Error('Binder Error: Referenced column "hold_code" not found');
  const text = errorText(e, '\n    at Drawer');
  assert.match(text, /^Error: Binder Error: Referenced column "hold_code" not found/);
  assert.match(text, /Component stack:\n {4}at Drawer$/);
  assert.equal(errorText('plain'), 'plain');
  clearErrors();
  report(e, 'render');
  report(e, 'render');
  for (let i = 0; i < 7; i++) report(new Error(`e${i}`), 'uncaught error');
  clearErrors();
});
