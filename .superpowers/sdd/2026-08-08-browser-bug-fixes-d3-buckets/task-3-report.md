# Task 3 Report — Five-Minute Metric Buckets

## Implemented

- Added a D3- and React-independent metric bucket model with 24 chronological
  five-minute buckets. It aligns the active bucket as `[floor(now),
  floor(now) + five minutes)`, keys source snapshots by `window_end`, replaces
  duplicate ends with the later snapshot, and zero-fills missing buckets.
- Added explicit `Asia/Jakarta` start and range label formatters. Their output
  does not depend on the process timezone.
- Added the requested runtime `d3` dependency and `@types/d3` development
  declaration package with their lockfile graph.

## TDD Evidence

### RED

The test file was created before `metricBuckets.ts`. The focused Node 22 test
was run in an explicitly non-Jakarta timezone:

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'
$env:DOCKER_CONFIG = $dockerConfigPath
New-Item -ItemType Directory -Force -Path $dockerConfigPath | Out-Null
docker run --rm -e TZ=America/New_York -v "${PWD}:/workspace" -w /workspace/web node:22-bookworm node --test src/lib/metricBuckets.test.ts
```

Exit code: `1`

```text
Error [ERR_MODULE_NOT_FOUND]: Cannot find module '/workspace/web/src/lib/metricBuckets.ts'
...
# fail 1
```

This is the expected failure: the public bucket helper module did not yet
exist.

### GREEN

After the minimal pure implementation, the same command completed with exit
code `0`:

```text
# tests 5
# pass 5
# fail 0
```

The tests use literal, hand-derived expectations for the approved sequence
`07:00–07:05 = 2`, `07:05–07:10 = 0`, and `07:10–07:15 = 1`; latest-snapshot
replacement; the exact 07:10 boundary; invalid/out-of-range ends; 24 ordered
buckets; and Jakarta labels including `23:55–00:00 WIB`.

## Dependency Installation

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'
$env:DOCKER_CONFIG = $dockerConfigPath
New-Item -ItemType Directory -Force -Path $dockerConfigPath | Out-Null
docker run --rm -v "${PWD}:/workspace" -w /workspace/web node:22-bookworm bash -lc "npm install --package-lock-only --ignore-scripts d3@latest; npm install --package-lock-only --ignore-scripts --save-dev @types/d3@latest"
```

This added `d3@^7.9.0` and `@types/d3@^7.4.3`. npm reported four existing
audit findings (one moderate and three high); the command did not update any
unrelated direct dependencies.

## Final Verification

```powershell
$dockerConfigPath = Join-Path $env:TEMP 'phase4-live-docker-config'
$env:DOCKER_CONFIG = $dockerConfigPath
New-Item -ItemType Directory -Force -Path $dockerConfigPath | Out-Null
docker run --rm -e TZ=America/New_York -v "${PWD}:/workspace" -v phase4-level3-cep-web-node-modules:/workspace/web/node_modules -w /workspace/web node:22-bookworm bash -lc "set -e; npm ci --ignore-scripts; node --test src/lib/*.test.ts; npm run build"
```

Exit code: `0`.

```text
# tests 28
# pass 28
# fail 0
...
✓ built in 510ms
```

The first full-build attempt exposed only a test-source compatibility issue:
`Array.prototype.at` is outside this project's ES2020 library target. The
tests were changed to equivalent `length - N` indexing, then the complete
command above passed. No production behavior changed during that correction.

## Files Changed

- `web/src/lib/metricBuckets.ts` — pure bucket types, alignment, normalization,
  timestamp validation, and explicit Jakarta formatters.
- `web/src/lib/metricBuckets.test.ts` — independent Node tests for the model
  and formatting contract.
- `web/package.json` — `d3` and `@types/d3` declarations.
- `web/package-lock.json` — corresponding resolved dependency graph.
- `.superpowers/sdd/2026-08-08-browser-bug-fixes-d3-buckets/task-3-report.md`
  — this report.

## Self-Review

- The production module imports neither D3 nor React and accepts a structural
  snapshot type compatible with the selected `WindowStat` values.
- `window_end` remains the authoritative bucket end; each output start is
  always exactly five minutes earlier.
- The latest received snapshot overwrites a matching end in the map, while
  ends not present in the expected active 24-window range have no effect.
- `Intl.DateTimeFormat` sets `timeZone: 'Asia/Jakarta'` directly; the tests run
  with `TZ=America/New_York` to prove machine-zone independence.
- `git diff --check` passed after removing build-generated artifacts, leaving
  only Task 3 source, dependency, lockfile, and report changes.

## Concerns

None for Task 3. D3 is deliberately installed but not imported until Task 4.
