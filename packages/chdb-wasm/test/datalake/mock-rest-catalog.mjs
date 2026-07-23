// Minimal Iceberg REST catalog for the DataLakeCatalog wasm tests. Implements
// the subset ClickHouse's RestCatalog actually calls:
//   POST /v1/oauth/tokens                   -> client_credentials grant (when `auth` is set)
//   GET  /v1/config?warehouse=...           -> {defaults:{}, overrides:{}}
//   GET  /v1/namespaces[?parent=...]        -> namespace listing (paginated)
//   GET  /v1/namespaces/{ns}/tables         -> table identifier listing (paginated)
//   GET  /v1/namespaces/{ns}/tables/{table} -> LoadTableResult (+ vended credentials)
// CORS is fully open so the same mock serves the browser fixture.
//
// Options beyond {port, descriptor}:
//   auth: {clientId, clientSecret, tokenMaxUses?} — enable OAuth: every catalog
//         request must carry `Authorization: Bearer <token>` issued by
//         /v1/oauth/tokens; a token expires after tokenMaxUses requests (401,
//         forcing the client's refresh-and-retry path).
//   vend: {accessKey, secretKey, endpoint} — storage credentials are vended in
//         LoadTableResult.config, and ONLY when the client sends
//         `X-Iceberg-Access-Delegation: vended-credentials`.
//   pageSize: N — namespaces/tables listings return N items per page with
//         next-page-token / pageToken pagination.
//   extraNamespaces: ['other', ...] — additional (empty) namespaces.
//
// startMockCatalog() resolves to the http.Server with a `.stats` property:
// {oauthTokensIssued, unauthorized, delegationHeaderSeen, maxPageRequests}.

import http from 'node:http';

export function startMockCatalog({ port, descriptor, auth, vend, pageSize, extraNamespaces = [] }) {
  const { namespace } = descriptor;
  const tables = descriptor.tables
    ?? [{ name: descriptor.table, metadata_location: descriptor.metadata_location, metadata: descriptor.metadata }];
  const namespaces = [namespace, ...extraNamespaces];

  const stats = { oauthTokensIssued: 0, unauthorized: 0, delegationHeaderSeen: 0, maxPageRequests: 0 };
  // Multiple tokens can be live at once (the client lists tables from a thread
  // pool; a refresh on one thread must not invalidate another thread's token —
  // real OAuth servers behave this way). Each token carries its own use budget.
  const tokenUses = new Map();

  const paginate = (items, url, key, shape) => {
    if (!pageSize || items.length <= pageSize) return { [key]: items.map(shape) };
    const start = Number(url.searchParams.get('pageToken') || 0);
    stats.maxPageRequests = Math.max(stats.maxPageRequests, Math.ceil(items.length / pageSize));
    const page = items.slice(start, start + pageSize);
    const body = { [key]: page.map(shape) };
    if (start + pageSize < items.length) body['next-page-token'] = String(start + pageSize);
    return body;
  };

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
        'Access-Control-Expose-Headers': '*',
        'Access-Control-Allow-Private-Network': 'true',
        'Cross-Origin-Resource-Policy': 'cross-origin',
      });
      res.end();
      return;
    }

    // --- OAuth token endpoint (no bearer required) ---
    if (auth && path === '/v1/oauth/tokens' && req.method === 'POST') {
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const params = new URLSearchParams(body);
        if (params.get('grant_type') !== 'client_credentials'
          || params.get('client_id') !== auth.clientId
          || params.get('client_secret') !== auth.clientSecret) {
          stats.unauthorized += 1;
          return send(401, { error: 'invalid_client' });
        }
        stats.oauthTokensIssued += 1;
        const token = `tok-${stats.oauthTokensIssued}`;
        tokenUses.set(token, 0);
        send(200, { access_token: token, token_type: 'bearer', expires_in: 3600 });
      });
      return;
    }

    // --- bearer enforcement for every other catalog call ---
    if (auth) {
      const token = (req.headers.authorization || '').replace(/^Bearer /, '');
      const uses = tokenUses.get(token);
      if (uses === undefined || (auth.tokenMaxUses && uses >= auth.tokenMaxUses)) {
        // An expired (over-used) token must 401 so the client refreshes and retries.
        tokenUses.delete(token);
        stats.unauthorized += 1;
        return send(401, { error: { message: 'invalid or expired token', type: 'NotAuthorizedException', code: 401 } });
      }
      tokenUses.set(token, uses + 1);
    }

    if (path === '/v1/config')
      return send(200, { defaults: {}, overrides: {} });

    if (path === '/v1/namespaces') {
      const parent = url.searchParams.get('parent');
      if (parent) return send(200, { namespaces: [] });
      return send(200, paginate(namespaces, url, 'namespaces', (n) => [n]));
    }

    for (const ns of namespaces) {
      if (path === `/v1/namespaces/${ns}/tables`) {
        const nsTables = ns === namespace ? tables : [];
        return send(200, paginate(nsTables, url, 'identifiers', (t) => ({ namespace: [ns], name: t.name })));
      }
    }

    const table = tables.find((t) => path === `/v1/namespaces/${namespace}/tables/${t.name}`);
    if (table) {
      const result = { 'metadata-location': table.metadata_location, metadata: table.metadata, config: {} };
      if (vend && req.headers['x-iceberg-access-delegation'] === 'vended-credentials') {
        stats.delegationHeaderSeen += 1;
        result.config = {
          's3.access-key-id': vend.accessKey,
          's3.secret-access-key': vend.secretKey,
          's3.endpoint': vend.endpoint,
        };
      }
      return send(200, result);
    }

    return send(404, { error: { message: `not found: ${req.method} ${req.url}`, type: 'NoSuchTableException', code: 404 } });
  });
  server.stats = stats;

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
