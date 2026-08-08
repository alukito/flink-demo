# Task 1 report — browser state helpers

## Implemented

- Added an immutable `appendUniqueEvent` helper that examines only the bounded
  event feed, ignores repeated `event_id` values, prepends new events, and
  retains at most 100 entries through `MAX_LIVE_EVENTS`.
- Added reducer-based `cartItemCount`, which sums line quantities.
- Added tab-storage session helpers for reading, writing, clearing, and exact
  role authorization. Stored roles are validated against the `Role` union
  instead of being cast from arbitrary storage text.
- Added the `test:browser-state` Node test script and focused behavior tests.

No React context or route wiring was changed; that remains Task 2 and Task 6
work.

## Files changed

- `web/src/lib/eventFeed.ts`
- `web/src/lib/eventFeed.test.ts`
- `web/src/lib/cart.ts`
- `web/src/lib/cart.test.ts`
- `web/src/lib/session.ts`
- `web/src/lib/session.test.ts`
- `web/package.json`

## TDD record

The three test files were created before their production helper modules. The
initial Node 22 attempt was blocked while Docker was unavailable. Once Docker
access was restored, the new helper modules were removed while the tests stayed
in place, producing the following real RED result.

### RED

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'
$env:DOCKER_CONFIG = $dockerConfigPath
docker run --rm -v "${PWD}:/workspace" -w /workspace/web node:22-bookworm node --test src/lib/eventFeed.test.ts src/lib/cart.test.ts src/lib/session.test.ts
```

Exit code: `1`

```text
TAP version 13
# node:internal/modules/esm/resolve:275
#     throw new ERR_MODULE_NOT_FOUND(
#           ^
# Error [ERR_MODULE_NOT_FOUND]: Cannot find module '/workspace/web/src/lib/cart.ts' imported from /workspace/web/src/lib/cart.test.ts
#   code: 'ERR_MODULE_NOT_FOUND',
#   url: 'file:///workspace/web/src/lib/cart.ts'
# }
# Node.js v22.23.2
# Subtest: src/lib/cart.test.ts
not ok 1 - src/lib/cart.test.ts
  ---
  duration_ms: 190.189436
  type: 'test'
  location: '/workspace/web/src/lib/cart.test.ts:1:1'
  failureType: 'testCodeFailure'
  exitCode: 1
  error: 'test failed'
  code: 'ERR_TEST_FAILURE'
  ...
# Error [ERR_MODULE_NOT_FOUND]: Cannot find module '/workspace/web/src/lib/eventFeed.ts' imported from /workspace/web/src/lib/eventFeed.test.ts
#   code: 'ERR_MODULE_NOT_FOUND',
#   url: 'file:///workspace/web/src/lib/eventFeed.ts'
# }
# Node.js v22.23.2
# Subtest: src/lib/eventFeed.test.ts
not ok 2 - src/lib/eventFeed.test.ts
  ---
  duration_ms: 197.757011
  type: 'test'
  location: '/workspace/web/src/lib/eventFeed.test.ts:1:1'
  failureType: 'testCodeFailure'
  exitCode: 1
  error: 'test failed'
  code: 'ERR_TEST_FAILURE'
  ...
# Error [ERR_MODULE_NOT_FOUND]: Cannot find module '/workspace/web/src/lib/session.ts' imported from /workspace/web/src/lib/session.test.ts
#   code: 'ERR_MODULE_NOT_FOUND',
#   url: 'file:///workspace/web/src/lib/session.ts'
# }
# Node.js v22.23.2
# Subtest: src/lib/session.test.ts
not ok 3 - src/lib/session.test.ts
  ---
  duration_ms: 197.886372
  type: 'test'
  location: '/workspace/web/src/lib/session.test.ts:1:1'
  failureType: 'testCodeFailure'
  exitCode: 1
  error: 'test failed'
  code: 'ERR_TEST_FAILURE'
  ...
1..3
# tests 3
# suites 0
# pass 0
# fail 3
# cancelled 0
# skipped 0
# todo 0
# duration_ms 236.697659
```

The helper modules were then restored from the test requirements and the same
Node 22 command produced GREEN:

### GREEN

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'
$env:DOCKER_CONFIG = $dockerConfigPath
docker run --rm -v "${PWD}:/workspace" -w /workspace/web node:22-bookworm node --test src/lib/eventFeed.test.ts src/lib/cart.test.ts src/lib/session.test.ts
```

Exit code: `0`

```text
TAP version 13
# Subtest: sums quantities across product lines instead of counting lines
ok 1 - sums quantities across product lines instead of counting lines
# Subtest: prepends a new event envelope without changing the retained events
ok 2 - prepends a new event envelope without changing the retained events
# Subtest: ignores a repeated event ID even when its payload differs
ok 3 - ignores a repeated event ID even when its payload differs
# Subtest: retains at most one hundred newest unique events
ok 4 - retains at most one hundred newest unique events
# Subtest: reads and clears all three session fields
ok 5 - reads and clears all three session fields
# Subtest: authorizes only a complete session with the requested role
ok 6 - authorizes only a complete session with the requested role
1..6
# tests 6
# suites 0
# pass 6
# fail 0
# cancelled 0
# skipped 0
# todo 0
# duration_ms 222.412583
```

The host Node is `v16.13.2`, so the test suite intentionally runs in the
Node 22 container. As an additional static type gate, the local TypeScript
compiler did run:

```powershell
node node_modules\typescript\lib\tsc.js --noEmit
```

Exit code: `0`

Output: none.

## Self-review

- Event tests use complete `EventEnvelope` shapes and a repeated ID with a
  deliberately different payload, proving comparison is by ID instead of
  payload equality.
- The event retention assertion uses literal 100-entry boundary expectations;
  no production helper derives expected values.
- The cart test verifies quantity total `4` across two lines (`3 + 1`), not a
  product-line count.
- Session tests use a minimal in-memory `StorageLike` implementation and check
  read/write/clear of all three keys, missing records, mismatched roles, and an
  invalid stored role.
- Production helpers are pure except for their explicitly injected/defaulted
  browser storage boundary. No mock behavior is asserted.
- `git diff --check` completed with exit code `0`; Git emitted only the
  pre-existing CRLF conversion warning for `web/package.json`.

## Concerns

None for Task 1. React integration is intentionally deferred to Tasks 2 and 6.

## Fix round 1 — enforce the live-feed hard cap

### Changed behavior

`appendUniqueEvent` now clamps a caller-supplied `maxEvents` value to
`MAX_LIVE_EVENTS`. A value such as `101` can no longer produce a 101-entry
feed; callers can still request a smaller cap. The regression coverage is in
`web/src/lib/eventFeed.test.ts`.

### RED

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'
$env:DOCKER_CONFIG = $dockerConfigPath
docker run --rm -v "${PWD}:/workspace" -w /workspace/web node:22-bookworm node --test src/lib/eventFeed.test.ts src/lib/cart.test.ts src/lib/session.test.ts
```

Exit code: `1`

```text
TAP version 13
# Subtest: sums quantities across product lines instead of counting lines
ok 1 - sums quantities across product lines instead of counting lines
# Subtest: prepends a new event envelope without changing the retained events
ok 2 - prepends a new event envelope without changing the retained events
# Subtest: ignores a repeated event ID even when its payload differs
ok 3 - ignores a repeated event ID even when its payload differs
# Subtest: retains at most one hundred newest unique events
ok 4 - retains at most one hundred newest unique events
# Subtest: clamps an oversized requested cap to one hundred events
not ok 5 - clamps an oversized requested cap to one hundred events
  ---
  duration_ms: 1.066882
  type: 'test'
  location: '/workspace/web/src/lib/eventFeed.test.ts:45:1'
  failureType: 'testCodeFailure'
  error: |-
    Expected values to be strictly equal:

    101 !== 100

  code: 'ERR_ASSERTION'
  name: 'AssertionError'
  expected: 100
  actual: 101
  operator: 'strictEqual'
  ...
# Subtest: reads and clears all three session fields
ok 6 - reads and clears all three session fields
# Subtest: authorizes only a complete session with the requested role
ok 7 - authorizes only a complete session with the requested role
1..7
# tests 7
# suites 0
# pass 6
# fail 1
# cancelled 0
# skipped 0
# todo 0
# duration_ms 213.837718
```

### GREEN

The same command after clamping the effective cap completed with exit code
`0`:

```text
TAP version 13
# Subtest: sums quantities across product lines instead of counting lines
ok 1 - sums quantities across product lines instead of counting lines
# Subtest: prepends a new event envelope without changing the retained events
ok 2 - prepends a new event envelope without changing the retained events
# Subtest: ignores a repeated event ID even when its payload differs
ok 3 - ignores a repeated event ID even when its payload differs
# Subtest: retains at most one hundred newest unique events
ok 4 - retains at most one hundred newest unique events
# Subtest: clamps an oversized requested cap to one hundred events
ok 5 - clamps an oversized requested cap to one hundred events
# Subtest: reads and clears all three session fields
ok 6 - reads and clears all three session fields
# Subtest: authorizes only a complete session with the requested role
ok 7 - authorizes only a complete session with the requested role
1..7
# tests 7
# suites 0
# pass 7
# fail 0
# cancelled 0
# skipped 0
# todo 0
# duration_ms 232.917141
```

### Commit

Recorded in the follow-up Task 1 review-fix commit.
