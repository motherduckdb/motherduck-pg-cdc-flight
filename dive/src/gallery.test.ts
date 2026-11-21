import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as esbuild from 'esbuild';
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

test('the dev copy gallery never reaches the Dive bundle', () => {
  // Built as scripts/bundle-dive.ts builds it: the same entry and externals, in memory.
  const bundler = readFileSync(resolve(root, 'scripts', 'bundle-dive.ts'), 'utf-8');
  assert.match(bundler, /entryPoints: \[resolve\(root, 'src', 'index\.tsx'\)\]/);
  const externals = JSON.parse(/const EXTERNALS = (\[[^\]]*\])/.exec(bundler)![1].replace(/'/g, '"'));
  const out = esbuild.buildSync({ entryPoints: [resolve(root, 'src', 'index.tsx')], bundle: true, write: false, jsx: 'preserve', format: 'esm', external: externals, logLevel: 'silent' });
  const text = out.outputFiles[0].text;
  assert.ok(text.length > 10_000);
  assert.doesNotMatch(text, /copy gallery|Gallery\b|gallery\//i);
  // and nothing under src/ imports it
  for (const f of readdirSync(resolve(root, 'src'), { recursive: true }) as string[])
    if (/\.tsx?$/.test(f) && !f.endsWith('gallery.test.ts'))
      assert.doesNotMatch(readFileSync(resolve(root, 'src', f), 'utf-8'), /from ['"][./]*gallery/, f);
});
