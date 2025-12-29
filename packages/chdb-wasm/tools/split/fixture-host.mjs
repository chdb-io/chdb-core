#!/usr/bin/env node
// Fixture services for the profiling run, hosted in a SEPARATE process: the
// profiling runner executes queries synchronously on its main thread, so any
// HTTP service living in the runner's event loop would deadlock the moment a
// query fetches from it (the wasm HTTP bridge blocks the thread that would
// serve the response). moto (python) is already its own process; this hosts
// the pure-Node pieces:
//   * a static file server for url() statements ({HTTP})
//   * the mock Iceberg REST catalog ({CATALOG}, reused from test/datalake/)
//   * the mock Unity catalog ({UNITY})
//
// Usage:
//   fixture-host.mjs --http-port N --static-dir DIR
//       [--catalog-port N --iceberg-descriptor FILE]
//       [--unity-port N --unity-descriptor FILE]
// Prints "READY" on stdout once everything listens; exits with the parent.

import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';

const args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].replace(/^--/, '')] = process.argv[i + 1];

const testDir = join(dirname(fileURLToPath(import.meta.url)), '../../test');
const { startMockCatalog } = await import(join(testDir, 'datalake/mock-rest-catalog.mjs'));
const { startMockUnityCatalog } = await import(join(testDir, 'datalake/mock-unity-catalog.mjs'));

function listenOrReject(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      server.removeListener('error', reject);
      resolve(server);
    });
  });
}

const jobs = [];

if (args['http-port']) {
  const dir = args['static-dir'];
  const server = createServer((req, res) => {
    try {
      const rel = normalize(decodeURIComponent((req.url || '/').split('?')[0])).replace(/^([/\\]|\.\.[/\\])+/, '');
      const body = readFileSync(join(dir, rel));
      res.setHeader('Content-Length', body.length);
      res.end(body);
    } catch {
      res.statusCode = 404;
      res.end('not found');
    }
  });
  jobs.push(listenOrReject(server, Number(args['http-port'])));
}

if (args['catalog-port']) {
  const descriptor = JSON.parse(readFileSync(args['iceberg-descriptor'], 'utf8'));
  jobs.push(startMockCatalog({ port: Number(args['catalog-port']), descriptor }));
}

if (args['unity-port']) {
  const descriptor = JSON.parse(readFileSync(args['unity-descriptor'], 'utf8'));
  jobs.push(startMockUnityCatalog({ port: Number(args['unity-port']), descriptor }));
}

await Promise.all(jobs);
console.log('READY');
