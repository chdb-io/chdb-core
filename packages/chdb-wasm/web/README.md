# chDB-wasm browser shell (Cloudflare deploy)

A static web page that runs the chDB WebAssembly engine fully in the browser — SQL
executes on the client, with no backend. It is deployed to Cloudflare Pages (the small
files) plus R2 (the large `.wasm`).

## Files

- `index.html` — the shell page. It references `./dist/...`; the deploy step rewrites
  `./dist` to a content-hashed `./dist-<hash>/` directory so it can be cached forever.
- `_headers` — COOP/COEP (cross-origin isolation, needed by the multi-threaded build)
  plus cache rules: immutable for the hashed engine dir, `no-cache` for `index.html`.
- `_routes.json` — routes only the engine dir (`/dist-*`) through the Pages Function.
- `functions/[[path]].js` — Pages Function that streams `*.wasm` from the R2 bucket
  (binding `WASM_BUCKET`), because the ~95 MB wasm exceeds the Pages 25 MB/file limit.

The engine files (the glue JS and the `.wasm`) are NOT committed here. They are build
outputs of the `chdb-wasm` package and are fetched at deploy time.

## Deploy

Manual only, via `.github/workflows/deploy-wasm-shell.yml` (a `workflow_dispatch`
trigger — it never runs on push, PR, merge, or tag). It fetches the engine + glue from
the `chdb-wasm` npm package, assembles the bundle with `../scripts/build-cf-bundle.sh`,
uploads the wasm to R2, and deploys the rest to Pages.

One-time prerequisites (by the Cloudflare account owner):

- Repo secrets: `CLOUDFLARE_API_TOKEN` (permissions: Cloudflare Pages — Edit, and
  Workers R2 Storage — Edit) and `CLOUDFLARE_ACCOUNT_ID`.
- An R2 bucket, bound to the Pages project as `WASM_BUCKET`.
- The Pages custom domain (e.g. `wasm.chdb.io`).
