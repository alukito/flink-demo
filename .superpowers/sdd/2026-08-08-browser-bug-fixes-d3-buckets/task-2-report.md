# Task 2 Report: WebSocket lifecycle ownership and live-feed deduplication

## Scope

Implemented only Task 2 in the assigned worktree. No chart, session, cart UI, or Flink files were changed.

## Root cause and approach

The previous hook stored the current socket and reconnect timer in refs shared by all effect instances. Cleanup closed the ref's current socket, but every `onclose` handler scheduled `connect` unconditionally. In React StrictMode or after an effect replacement, a disposed or stale socket could therefore set connection state and schedule an orphan reconnection.

Each effect now owns its `disposed` flag, current socket, and reconnect timer. `shouldReconnect` is a pure lifecycle decision that permits a one-second retry only for an active socket in a live effect with a token. All state and event callbacks additionally ignore disposed or stale sockets. Cleanup marks the effect disposed before clearing its timer and closing its owned socket.

`EventProvider` now delegates to Task 1's immutable `appendUniqueEvent` helper, so duplicate `event_id` values are discarded and the live feed remains bounded to the newest 100 unique events.

## Red-green TDD evidence

### RED

Command (run from the assigned worktree):

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'; $env:DOCKER_CONFIG = $dockerConfigPath; New-Item -ItemType Directory -Force -Path $dockerConfigPath | Out-Null; docker run --rm -v "${PWD}:/workspace" -v phase4-level3-cep-web-node-modules:/workspace/web/node_modules -w /workspace/web node:22-bookworm bash -lc "npm ci --ignore-scripts; node --test src/lib/webSocketLifecycle.test.ts"
```

Relevant output:

```text
Error [ERR_MODULE_NOT_FOUND]: Cannot find module '/workspace/web/src/lib/webSocketLifecycle.ts'
...
# fail 1
```

The new lifecycle test suite failed because the pure production helper did not exist.

### GREEN: lifecycle helper

Command:

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'; $env:DOCKER_CONFIG = $dockerConfigPath; New-Item -ItemType Directory -Force -Path $dockerConfigPath | Out-Null; docker run --rm -v "${PWD}:/workspace" -v phase4-level3-cep-web-node-modules:/workspace/web/node_modules -w /workspace/web node:22-bookworm node --test src/lib/webSocketLifecycle.test.ts
```

Relevant output:

```text
# tests 4
# pass 4
# fail 0
```

### GREEN: focused lifecycle and feed suites

Command:

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'; $env:DOCKER_CONFIG = $dockerConfigPath; New-Item -ItemType Directory -Force -Path $dockerConfigPath | Out-Null; docker run --rm -v "${PWD}:/workspace" -v phase4-level3-cep-web-node-modules:/workspace/web/node_modules -w /workspace/web node:22-bookworm node --test src/lib/webSocketLifecycle.test.ts src/lib/eventFeed.test.ts
```

Relevant output:

```text
# tests 8
# pass 8
# fail 0
```

## Final verification

Command:

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'; $env:DOCKER_CONFIG = $dockerConfigPath; New-Item -ItemType Directory -Force -Path $dockerConfigPath | Out-Null; docker run --rm -v "${PWD}:/workspace" -v phase4-level3-cep-web-node-modules:/workspace/web/node_modules -w /workspace/web node:22-bookworm bash -lc "set -e; node --test src/lib/*.test.ts; npx tsc -b; npm run build"
```

Relevant output:

```text
# tests 23
# pass 23
# fail 0

> web@0.0.0 build
> tsc -b && vite build

✓ built in 474ms
```

`npx tsc -b` completed with exit code 0 and no diagnostics before the build command ran.

## Files changed

- `web/src/lib/webSocketLifecycle.ts` — pure reconnect-decision helper.
- `web/src/lib/webSocketLifecycle.test.ts` — lifecycle decision regression tests.
- `web/src/hooks/useWebSocket.ts` — effect-local socket, timer, and disposal ownership.
- `web/src/context/EventContext.tsx` — reuse `appendUniqueEvent` for immutable bounded event-feed deduplication.
- `.superpowers/sdd/2026-08-08-browser-bug-fixes-d3-buckets/task-2-report.md` — this report.

## Self-review

- Verified `onclose` calls `shouldReconnect` with the effect's disposed state, the closing socket's ownership check, and token presence.
- Verified stale `onopen`, `onmessage`, and `onerror` callbacks cannot alter active state or feed data.
- Verified cleanup sets `disposed = true` before closing the socket and clears the per-effect timer.
- Verified the hook effect depends only on `token`; the event callback stays current through its ref, avoiding a reconnect on callback rerender.
- Verified `EventProvider` uses the Task 1 public helper rather than duplicating cap or deduplication logic.
- Ran `git diff --check` with no whitespace errors.

## Concerns

None. Docker's `npm ci` reported pre-existing dependency audit findings (1 moderate, 3 high); no dependency versions were changed for this task.
