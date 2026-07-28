// Minimal Unity catalog serving exactly one Delta table, for the DataLakeCatalog
// wasm tests. Implements the subset ClickHouse's UnityCatalog client calls
// (base url = http://host:port/api/2.1/unity-catalog):
//   GET {base}/schemas?catalog_name=W          -> schema listing
//   GET {base}/tables?catalog_name=W&schema_name=S -> table name listing
//   GET {base}/tables/{W}.{S}.{T}              -> table info (storage_location,
//       securable_kind/data_source_format, columns[] with Unity's type_json)
// CORS-open like the Iceberg mock so a browser fixture can reuse it.

import http from 'node:http';

export function startMockUnityCatalog({ port, descriptor }) {
  const { catalog, schema, table, location, columns } = descriptor;
  const base = '/api/2.1/unity-catalog';

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://127.0.0.1:${port}`);
    const path = url.pathname.replace(/\/+$/, '');

    const send = (code, body) => {
      const payload = JSON.stringify(body);
      res.writeHead(code, {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': req.headers['access-control-request-headers'] || '*',
        'Access-Control-Expose-Headers': '*',
        'Access-Control-Allow-Private-Network': 'true',
        'Cross-Origin-Resource-Policy': 'cross-origin',
      });
      res.end(payload);
    };

    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': req.headers['access-control-request-headers'] || '*',
      });
      res.end();
      return;
    }

    if (path === `${base}/schemas`)
      return send(200, { schemas: [{ catalog_name: catalog, full_name: `${catalog}.${schema}` }] });

    if (path === `${base}/tables`)
      return send(200, { tables: [{ name: table }] });

    if (path === `${base}/tables/${catalog}.${schema}.${table}`)
      return send(200, {
        name: table,
        storage_location: location,
        securable_kind: 'TABLE_DELTA_EXTERNAL',
        data_source_format: 'DELTA',
        columns,
        table_id: '11111111-2222-3333-4444-555555555555',
      });

    return send(404, { error_code: 'NOT_FOUND', message: `not found: ${req.method} ${req.url}` });
  });

  // Reject on listen errors (e.g. EADDRINUSE from a leaked process) so callers'
  // try/finally cleanup runs instead of the process dying on an unhandled 'error'.
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      server.removeListener('error', reject);
      resolve(server);
    });
  });
}
