// Minimal Iceberg REST catalog serving exactly one table, for the DataLakeCatalog
// wasm tests. Implements the subset ClickHouse's RestCatalog actually calls:
//   GET /v1/config?warehouse=...            -> {defaults:{}, overrides:{}}
//   GET /v1/namespaces[?parent=...]         -> namespace listing (one level)
//   GET /v1/namespaces/{ns}/tables          -> table identifier listing
//   GET /v1/namespaces/{ns}/tables/{table}  -> LoadTableResult (metadata-location + metadata)
// CORS is fully open so the same mock serves the browser fixture.

import http from 'node:http';

export function startMockCatalog({ port, descriptor }) {
  const { namespace, table, metadata_location, metadata } = descriptor;

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
        'Access-Control-Allow-Headers': '*',
      });
      res.end(payload);
    };

    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': '*',
      });
      res.end();
      return;
    }

    if (path === '/v1/config')
      return send(200, { defaults: {}, overrides: {} });

    if (path === '/v1/namespaces') {
      // Root listing returns our one namespace; child listings are empty.
      const parent = url.searchParams.get('parent');
      return send(200, { namespaces: parent ? [] : [[namespace]] });
    }

    if (path === `/v1/namespaces/${namespace}/tables`)
      return send(200, { identifiers: [{ namespace: [namespace], name: table }] });

    if (path === `/v1/namespaces/${namespace}/tables/${table}`)
      return send(200, { 'metadata-location': metadata_location, metadata, config: {} });

    return send(404, { error: { message: `not found: ${req.method} ${req.url}`, type: 'NoSuchTableException', code: 404 } });
  });

  return new Promise((resolve) => {
    server.listen(port, '127.0.0.1', () => resolve(server));
  });
}
