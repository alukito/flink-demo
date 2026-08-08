# Final review fix report

Date: 2026-08-08
Branch: `codex/phase4-level3-cep`

## Root-cause tracing (recorded before implementation)

### 1. Explicit `null` WebSocket override falls back to the user session

- Symptom: a Dashboard render whose dashboard token is not ready can open `/ws` with the current buyer, seller, or shipper token.
- Backward trace: `new WebSocket(...token=...)` receives `token` from `useWebSocket`; `Dashboard` calls `useWebSocket(onMessage, dashToken)` with initial `dashToken === null`; line 16 selects `overrideToken ?? sessionToken`; nullish coalescing treats both explicit `null` and omitted `undefined` as fallback requests.
- Original trigger: the hook API uses two distinct states (omitted means use the role session, explicit null means suppress connection) but the implementation collapses them.
- Working pattern/difference: the connection effect already suppresses falsy resolved tokens (`if (disposed || !token) return`), so the missing piece is preserving explicit null through token selection.
- Hypothesis: a resolver that falls back only when `overrideToken === undefined` will retain existing role-page behavior while preventing a pre-token Dashboard socket.

### 2. Persisted dashboard token is reused indefinitely

- Symptom: expired or invalid dashboard tokens reconnect every second and signing-key rotation cannot recover without storage cleanup.
- Backward trace: reconnect is driven by `shouldReconnect(...hasToken: Boolean(token))`; a stale token remains truthy; `Dashboard` initializes from `localStorage.getItem('dash_token')`; the session-creation effect exits early whenever that value exists; there is no expiry validation, rejection recovery, or renewal path.
- Original trigger: commit `5bccdca` isolated dashboard credentials by persisting `dash_token`, but persistence became the token-validity decision. The API issues 24-hour tokens, while browser storage and signing keys can outlive them.
- Hypothesis: mount with in-memory `null`, delete only the legacy `dash_token`, request a fresh dashboard session once per Dashboard mount, and install the returned token only while the mount remains live. Combined with finding 1, no user-session socket can open during renewal.

### 3. Embedded frontend is stale

- Symptom/evidence: tracked `app/web/dist/assets/index-DG9eFosW.js` contains `Sliding window history`; `app/web/dist/index.html` points to `index-DG9eFosW.js` and `index-C0FfZ6gF.css`.
- Backward trace: `app/web/embed.go` embeds `dist` at Go compile time; Vite is configured to write there with `emptyOutDir: true`; direct `go build` and `make dev-app` do not invoke Vite.
- Original trigger: the last commit touching `app/web/dist` is `78f5a2a` (2026-08-02), while current D3/session changes landed in `7566e0d`, `d91737c`, and `831573c` on 2026-08-08 without rebuilding the tracked artifact.
- Hypothesis: a fresh Node 22 Vite build will delete obsolete hashes and make direct Go builds embed current D3/aligned-window/sessionStorage behavior.

### 4. Canonical verification omits suites

- Symptom/evidence: seven files currently match `web/src/lib/*.test.ts`, including `metricBuckets.test.ts` and `webSocketLifecycle.test.ts`; package scripts enumerate only eventFeed/cart/session, jakartaDay, and cepAlerts. `make verify` invokes only `test:cep` and `test:jakarta`, so five suites are absent from that target and two are absent from every aggregate command.
- Original trigger: test scripts were added feature-by-feature as fixed file lists, and the Make target was not updated when new suites appeared.
- Hypothesis: one Node test glob script, invoked once by `make verify`, will discover all current library test files and remain complete as suites are added.

## RED / GREEN log

All frontend commands used Node 22.23.2 in `node:22-bookworm-slim` with `DOCKER_CONFIG` set to `%TEMP%\codex-phase4-docker-config`.

### Token selection and renewal

1. Tests were added first to `webSocketLifecycle.test.ts`. The first run failed at module load because the wished-for dashboard renewal helper did not yet exist (`ERR_MODULE_NOT_FOUND: dashboardToken.ts`).
2. Minimal behavior scaffolding then represented the two existing broken decisions (`overrideToken ?? sessionToken` and renewal without deleting persisted state), so the regression assertions could fail at behavior level before either production consumer changed.
3. RED command:

   `docker run --rm -v "${PWD}:/workspace" -w /workspace/web node:22-bookworm-slim node --test src/lib/webSocketLifecycle.test.ts`

   Exit 1, 8 tests / 6 pass / 2 fail. Exact failures:

   - `keeps an explicit null override...`: expected `null`, actual `buyer-token`.
   - `removes a persisted dashboard token...`: expected old-token presence `false`, actual `true` at session-request time.

4. Minimal GREEN changes:

   - `resolveWebSocketToken` falls back only when the override is exactly `undefined`.
   - `requestFreshDashboardToken` removes `dash_token` synchronously before calling the API.
   - `Dashboard` starts with in-memory `null`, requests once per mount, never persists the fresh token, and ignores late completion after unmount.
   - `useWebSocket` consumes the tested resolver.

5. GREEN: the same focused command exited 0 with 8/8 passing.

### Canonical frontend command

- RED command: `docker run ... node:22-bookworm-slim npm test`
- RED output: `npm error Missing script: "test"` (exit 1).
- GREEN implementation: package script `node --test src/lib/*.test.ts`; `make verify` invokes `cd web && npm test` once.
- GREEN command: `docker run ... node:22-bookworm-slim npm test`
- GREEN output: 32/32 passing, including metric-bucket cases 18–22 and WebSocket/token cases 25–32.
- Cross-shell glob proof: `node --test 'src/lib/*.test.ts'` (quoted so the Unix shell cannot expand it) also found and passed all 32 tests, confirming Node itself handles the pattern.

## Build and bundle evidence

- Dependency install: isolated Docker volume + `npm ci`, exit 0. It reported four known audit findings; no audit remediation was attempted per scope.
- Lint: `npm run lint`, exit 0, 0 errors and the same four pre-existing `react(only-export-components)` warnings in the two Context files.
- Build: `npm run build`, exit 0; TypeScript + Vite 8.1.5 transformed 609 modules and produced:

  - `app/web/dist/assets/index-Bc_ePpH_.js` (316.41 kB)
  - `app/web/dist/assets/index-BzRO8wGE.css` (3.95 kB)
  - updated `app/web/dist/index.html`

- Vite `emptyOutDir` removed obsolete `index-DG9eFosW.js` and `index-C0FfZ6gF.css`; the new index references only the new hashes.
- Generated-bundle checks:

  - contains `five-minute aligned-window history`;
  - contains `metric-bucket-chart-svg`, and the Edge render confirmed that SVG exists (D3-backed `MetricBarChart`);
  - contains `sessionStorage` (8 occurrences);
  - contains no `Sliding window history`;
  - contains one `localStorage` occurrence, whose only use is `removeItem('dash_token')`; there are no old user-session `localStorage` reads/writes for `token`, `name`, or `role`.
- Focused headless Microsoft Edge probe against the production bundle, with an expired legacy dashboard token and a buyer token pre-seeded, exited 0. Observed sequence:

  `session-request` → `session-response` → `ws:fresh-dashboard-token`

  It observed exactly one WebSocket token (`fresh-dashboard-token`), `dash_token === null`, the buyer session token unchanged in `sessionStorage`, connection text `Connected`, and a rendered D3 chart. This proves no socket opens before fresh issuance and no user-session fallback occurs.
- Go verification: Docker `golang:1.23-bookworm go test ./...` exited 0 for every package. A temporary focused `TestEmbeddedBundleProbe` (removed immediately after the run) requested `/` and `/assets/index-Bc_ePpH_.js` through the real Go router; both returned 200, the asset contained current aligned-window text, and it did not contain the old sliding-window text.

## Files and commits

Intended final files:

- `Makefile`
- `web/package.json`
- `web/src/hooks/useWebSocket.ts`
- `web/src/pages/Dashboard.tsx`
- `web/src/lib/dashboardToken.ts`
- `web/src/lib/webSocketLifecycle.ts`
- `web/src/lib/webSocketLifecycle.test.ts`
- `app/web/dist/index.html`
- replacement generated JS/CSS hashes (with the two stale hashes deleted)
- this report

Fix commit: `0588499 fix: renew dashboard websocket credentials`.

Report commit: the documentation-only commit immediately following `0588499` (its own hash cannot be embedded without changing that hash).

## Self-review

- Scope: no Flink or Go production behavior changed; only Web token lifecycle, canonical frontend verification, and generated assets changed.
- Token-state mutation checks: changing the resolver back to `??` fails the explicit-null test; omitting legacy removal fails while the fake creator is executing; changing dashboard-session name/role arguments fails their contract assertions.
- Lifecycle: the first render always passes explicit `null`; hook effect ordering therefore cannot open a role-session socket. The Edge probe independently demonstrated the request/socket order.
- Persistence: only legacy `dash_token` is removed. The fresh dashboard JWT exists solely in React state, so reload, expiry, and signing-key rotation all cause fresh issuance.
- Generated output: old hashes are deletions, new index hashes exist, and the real Go router served the newly named asset.
- Canonical command: a single glob covers all seven current `src/lib/*.test.ts` files without maintaining an enumerated list.
- Diff/status review found no package-lock churn, temporary probes, Go test files, Flink changes, or unrelated edits.

## Concerns

- The four pre-existing Fast Refresh lint warnings remain intentionally unchanged.
- `npm ci` reports 1 moderate and 3 high audit findings; remediation is explicitly outside this wave.
- If the fresh dashboard-session API request itself fails, the error is logged and the page stays disconnected until remount; no stale/user token is used. Automatic API-request retry was not part of the accepted fresh-on-mount architecture.
- Heavy Flink verification was not rerun, as authorized; no Flink files changed.
