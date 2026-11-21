// Adapted from motherduckdb/md-migration-assessment (dive/runtime/duckdb-proxy-plugin.ts, e4ae932), Copyright 2026 MotherDuck, Apache-2.0. Modified by MotherDuck, 2026; see dive/THIRD_PARTY.md.
/**
 * Dev-only Vite plugin: answers the dashboard's `api/query` and `api/info`
 * endpoints with Node DuckDB over a LOCAL collection, so the Dive source can be
 * previewed with hot reload and no MotherDuck connection.
 *
 *   CDC_OPS_DB=../.dive-preview/cdc_ops.duckdb npm run dev
 *
 * The file is attached READ_ONLY under the alias the Dive's REQUIRED_DATABASES
 * declares (`ops`).
 * This plugin never runs in the production bundle (apply: 'serve').
 */
import type { Plugin } from 'vite';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { resolve } from 'node:path';
import { existsSync } from 'node:fs';

const ALIAS = 'ops';

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((res, rej) => {
    let data = '';
    req.on('data', (c: Buffer) => (data += c.toString()));
    req.on('end', () => res(data));
    req.on('error', rej);
  });
}

function sendJson(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

export default function duckdbProxy(): Plugin {
  return {
    name: 'cdc-ops-local-duckdb-proxy',
    apply: 'serve',
    configureServer(server) {
      const dbPath = process.env.CDC_OPS_DB
        ? resolve(process.env.CDC_OPS_DB)
        : resolve(import.meta.dirname, '..', '..', '.dive-preview', 'cdc_ops.duckdb');
      if (!existsSync(dbPath)) {
        server.config.logger.warn(
          `[cdc-ops] no preview database at ${dbPath}; run scripts/dive_preview.py or set CDC_OPS_DB`,
        );
      }

      let ready: Promise<any> | null = null;
      const connect = () => {
        if (!ready) {
          ready = (async () => {
            const { DuckDBInstance } = await import('@duckdb/node-api');
            const instance = await DuckDBInstance.create(':memory:');
            const conn = await instance.connect();
            await conn.run(`ATTACH '${dbPath.replace(/'/g, "''")}' AS ${ALIAS} (READ_ONLY)`);
            await conn.run('SET enable_external_access = false');
            return conn;
          })();
        }
        return ready;
      };
      connect().catch((e) => server.config.logger.error(`[cdc-ops] ${e.message}`));

      server.middlewares.use(async (req, res, next) => {
        const url = (req.url ?? '').split('?')[0];
        if (url === '/api/info' && req.method === 'GET') {
          sendJson(res, 200, { mode: 'local', database: dbPath, alias: ALIAS });
          return;
        }
        if (url === '/api/query' && req.method === 'POST') {
          try {
            const { sql } = JSON.parse(await readBody(req));
            if (typeof sql !== 'string' || !sql.trim()) {
              sendJson(res, 400, { error: 'missing sql' });
              return;
            }
            const conn = await connect();
            const reader = await conn.runAndReadAll(sql);
            sendJson(res, 200, { data: reader.getRowObjectsJson() });
          } catch (err: any) {
            sendJson(res, 500, { error: err?.message ?? String(err) });
          }
          return;
        }
        next();
      });
    },
  };
}
