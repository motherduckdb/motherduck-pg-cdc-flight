// Adapted from motherduckdb/md-migration-assessment (dive/vite.config.ts, e4ae932), Copyright 2026 MotherDuck, Apache-2.0. Modified by MotherDuck, 2026; see dive/THIRD_PARTY.md.
import { defineConfig, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'node:path';
import duckdbProxy from './runtime/duckdb-proxy-plugin.ts';

// Dev only: preview the Dive against a local cdc_ops (CDC_OPS_DB=path.duckdb npm run dev);
// the proxy plugin answers api/query with Node DuckDB. The Dive itself ships as the
// single file `npm run bundle` writes.
//
// CDC_DIVE_FLIGHTS='{"phase2":{"table_action":"<uuid>","table_membership":"<uuid>"}}'
// enables the action buttons in the preview, against the simulated Flight runs in
// runtime/query-provider.tsx (as deploy_dive.py --flights does for the real Dive).
// CDC_DIVE_OWNER=<user> sets the owner (deploy_dive.py's --expect-user): the preview's
// current_user is `duckdb`, so CDC_DIVE_OWNER=duckdb previews the owner's view and any
// other name a read-only viewer's. Unset, the preview is read-only, as an owner-less deploy.
const previewFlights = (): Plugin => ({
  name: 'cdc-preview-flights',
  apply: 'serve',
  enforce: 'pre',
  transform(code, id) {
    const flights = process.env.CDC_DIVE_FLIGHTS;
    const owner = process.env.CDC_DIVE_OWNER;
    if (flights && id.endsWith('src/flights.ts')) return code.replace(/['"]__CDC_FLIGHTS__['"]/, JSON.stringify(flights));
    if (owner && id.endsWith('src/owner.ts')) return code.replace(/['"]__CDC_OWNER__['"]/, JSON.stringify(owner));
    return null;
  },
});

// CDC_DIVE_GALLERY_ONLY=1 serves only the dev copy gallery (gallery.html): the app's own
// page redirects there, so a gallery server is never mistaken for the preview.
const galleryOnly = (): Plugin => ({
  name: 'cdc-gallery-only',
  apply: 'serve',
  configureServer(server) {
    if (!process.env.CDC_DIVE_GALLERY_ONLY) return;
    server.middlewares.use((req, res, next) => {
      const path = (req.url ?? '/').split('?')[0];
      if (path === '/' || path === '/index.html') {
        res.statusCode = 302;
        res.setHeader('Location', '/gallery.html');
        res.end();
        return;
      }
      next();
    });
  },
});

export default defineConfig({
  base: './',
  resolve: {
    alias: {
      '@motherduck/react-sql-query': resolve(import.meta.dirname, 'runtime/query-provider.tsx'),
    },
  },
  plugins: [react(), duckdbProxy(), previewFlights(), galleryOnly()],
  server: { host: '127.0.0.1' },
});
