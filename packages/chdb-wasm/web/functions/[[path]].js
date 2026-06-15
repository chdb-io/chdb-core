// Pages Function: serve *.wasm from the R2 bucket (binding: WASM_BUCKET); everything
// else falls through to the static Pages assets. Same-origin, so cross-origin
// isolation (COEP) is satisfied. The ~95 MB wasm exceeds the Pages 25 MB/file limit,
// so it lives in R2 rather than in the Pages deployment.
export async function onRequest(context) {
  const { request, env, next } = context;
  const { pathname } = new URL(request.url);
  if (pathname.endsWith(".wasm")) {
    const key = pathname.replace(/^\/+/, "");          // e.g. "dist-XXXX/chdb.wasm"
    const obj = await env.WASM_BUCKET.get(key);
    if (!obj) return new Response("Not found: " + key, { status: 404 });
    return new Response(obj.body, {
      headers: {
        "Content-Type": "application/wasm",
        "Cache-Control": "public, max-age=31536000, immutable",
      },
    });
  }
  return next();
}
