import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ALIAS, PLACEHOLDER } from './share.ts';

test('the Dive declares the cdc_ops share as a share, under the alias its queries use', () => {
  const source = readFileSync(resolve(dirname(fileURLToPath(import.meta.url)), 'index.tsx'), 'utf-8');
  const literal = `export const REQUIRED_DATABASES = [{ type: 'share', path: '${PLACEHOLDER}', alias: '${ALIAS}' }];`;
  assert.ok(source.includes(literal), 'index.tsx must declare REQUIRED_DATABASES as ' + literal);
});
