# Final fix report — Phase 4 whole-branch review

## Scope and delivered fixes

Baseline was the clean linked worktree `codex/phase4-level3-cep` at
`0e06d836134a88decf5ca14e9e960cf1deb5b9b3`.

1. Added `AlertDeduplicator`, a shared output-boundary keyed process function
   with eight-hour processing-time TTL, `OnCreateAndWrite` updates, and
   `NeverReturnExpired` visibility. All five CEP patterns use it, preserving
   their existing state descriptor names. This removes the three unbounded
   pattern-local states and keeps the two already-bounded patterns on the same
   semantics.
2. Added an eight-hour `within` bound to `DeliveryCompletedPattern`. The class
   documents that this is an operational partial-match cleanup horizon aligned
   with the approved demo/cache horizon, not the one-minute slow-delivery rule.
   The pattern still uses match-only selection, so expiry emits no timeout
   alert. Regression cases cover a delivery at 7:59:59 with numeric 28,799
   elapsed seconds and cleanup at the exact eight-hour boundary.
3. Strengthened `scripts/phase4-smoke.sh` with a reusable runtime-capacity
   predicate. Startup readiness and the branch immediately before the success
   message both require exactly 12 RUNNING jobs plus exactly one TaskManager
   reporting 12 slots through `/taskmanagers`.

No SDD ledger, deferred-minor product code, frontend code, generated assets, or
unrelated files were modified.

## TDD and focused regression record

`AlertDeduplicatorTest` and the two delivery-retention cases were written before
the production changes. Executing RED/GREEN was blocked before test discovery:

```text
> mvn -f flink/pom.xml -Dtest='AlertDeduplicatorTest,DeliveryCompletedPatternTest' test
mvn: The term 'mvn' is not recognized as a name of a cmdlet, function, script file, or executable program.
Exit code: 1
```

There is no alternate local compiler:

```text
> java com.sun.tools.javac.Main -version
Error: Could not find or load main class com.sun.tools.javac.Main
Caused by: java.lang.ClassNotFoundException: com.sun.tools.javac.Main
Exit code: 1
```

The cached Flink dependencies therefore do not make the Java tests executable
in this worktree. No Maven pass is claimed.

## Verification evidence

### Shell and Compose

```text
> & 'C:\Program Files\Git\bin\sh.exe' -n scripts/phase4-smoke.sh
Exit code: 0
Output: none
```

```text
> $env:DOCKER_CONFIG = Join-Path $env:TEMP 'phase4-final-fix-docker-config'
> New-Item -ItemType Directory -Force $env:DOCKER_CONFIG | Out-Null
> docker compose config --quiet
Exit code: 0
Output: none
```

The exact TaskManager jq predicate was run against a valid fixture, two
TaskManagers, and an eleven-slot TaskManager:

```text
one_12_slot_taskmanager   : 0
two_taskmanagers_rejected : 1
eleven_slots_rejected     : 1
```

Static smoke-gate assertions passed:

```text
capacity_definition     : 1
startup_and_final_calls : 2
taskmanager_api_checks  : 1
exact_12_slot_checks    : 1
fresh_pre_success_gate  : True
```

### Java/source invariants

The focused PowerShell assertions checked all `*Pattern.java` files, the shared
deduplicator, and both new regression sources. Output:

```text
shared_deduplicator_uses : 5
local_deduplicators      : 0
output_ttl_hours         : 8
delivery_horizon_hours   : 8
timeout_alert_callback   : False
```

The assertions also required the TTL test to cover eight hours,
`OnCreateAndWrite`, and `NeverReturnExpired`, and required delivery fixtures at
7:59:59 and 8:00:00 with the literal numeric result `28_799L`.

### Unavailable executable gates

```text
> mvn -f flink/pom.xml clean verify
mvn: The term 'mvn' is not recognized as a name of a cmdlet, function, script file, or executable program.
Exit code: 1
```

```text
> docker info --format '{{.ServerVersion}}'
failed to connect to the docker API at npipe:////./pipe/docker_engine; the system cannot find the file specified.
Exit code: 1
```

The unavailable Docker daemon blocks live `scripts/phase4-smoke.sh`, fresh
runtime job/slot observation, and live alert emission. Frontend gates were not
rerun because this fix wave does not touch frontend sources or assets.

### Diff quality

```text
> git diff --check
Exit code: 0
Output: only existing LF-to-CRLF working-copy warnings; no whitespace errors
```

The final scope audit contains only the shared Java helper/test, the five CEP
pattern integrations, delivery-retention regression coverage,
`scripts/phase4-smoke.sh`, and this report.

## Remaining concern

Java compilation/tests and the live Compose smoke still require a host with
Maven/JDK tooling and a running Docker daemon. Static and configuration checks
passed, but they are not substitutes for those blocked executable gates.
