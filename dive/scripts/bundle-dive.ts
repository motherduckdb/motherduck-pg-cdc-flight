// Adapted from motherduckdb/md-migration-assessment (dive/scripts/bundle-dive.ts, e4ae932), Copyright 2026 MotherDuck, Apache-2.0. Modified by MotherDuck, 2026; see dive/THIRD_PARTY.md.
/**
 * Bundle the Dive source into the single file scripts/deploy_dive.py uploads.
 *
 * Everything under src/ is inlined,
 * the runtime-provided libraries stay external, JSX is preserved because the
 * Dive runtime compiles it.
 */
import * as esbuild from 'esbuild';
import { mkdirSync, readFileSync } from 'node:fs';
import { PLACEHOLDER } from '../src/share.ts';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '..');
const out = resolve(root, '.build', 'index.tsx');

const EXTERNALS = ['react', 'react-dom', 'react-dom/client', '@motherduck/react-sql-query', 'recharts', 'd3', 'lucide-react'];

mkdirSync(dirname(out), { recursive: true });
esbuild.buildSync({
  entryPoints: [resolve(root, 'src', 'index.tsx')],
  bundle: true,
  outfile: out,
  jsx: 'preserve',
  minify: false,
  format: 'esm',
  external: EXTERNALS,
  logLevel: 'info',
});

const source = readFileSync(out, 'utf-8');
// esbuild hoists exports into one `export { X as default, REQUIRED_DATABASES }` statement.
const hasDefault = /export\s+default\s/.test(source) || /export\s*\{[^}]*\bas\s+default\b[^}]*\}/.test(source);
const hasRequired = /export\s+const\s+REQUIRED_DATABASES/.test(source) || /export\s*\{[^}]*\bREQUIRED_DATABASES\b[^}]*\}/.test(source);
if (!hasDefault) throw new Error('bundled Dive is missing a default export');
if (!hasRequired) throw new Error('bundled Dive is missing REQUIRED_DATABASES');
if (!source.includes(PLACEHOLDER)) throw new Error(`bundled Dive is missing the ${PLACEHOLDER} placeholder`);
// scripts/deploy_dive.py --flights replaces this quoted placeholder (src/flights.ts). A
// read-only build bundles no action code, so no placeholder: deploy_dive then refuses --flights.
if (source.split('"__CDC_FLIGHTS__"').length > 2) throw new Error('bundled Dive must contain the Flights placeholder at most once');
// scripts/deploy_dive.py replaces the owner placeholder with --expect-user (src/owner.ts).
if (source.split('"__CDC_OWNER__"').length !== 2) throw new Error('bundled Dive must contain the owner placeholder exactly once');
console.log(`bundled Dive -> ${out} (${(source.length / 1024).toFixed(1)} KB)`);
