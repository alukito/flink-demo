# Jakarta Daily Metrics and Dashboard Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make daily Level 2 metrics follow Jakarta calendar days and replay the latest Level 2 values to dashboards that connect after those values were emitted.

**Architecture:** A small Java `DailyTime` utility owns Jakarta date-key and next-midnight conversion for Flink. The Go WebSocket hub keeps one defensive byte copy per allowed `(metric, scope)` pair and replays those values only to dashboard clients. React owns a Jakarta-day clock, ignores cached daily values from earlier Jakarta days, and displays an em dash until the first event of the current day.

**Tech Stack:** Java 11, Apache Flink 1.19.3, JUnit 5, Go 1.23+, Testify, React 19, TypeScript 6, Node built-in test runner, Docker Compose

## Global Constraints

- Use the IANA zone `Asia/Jakarta` for all daily date keys and boundaries.
- Event timestamps and `window_end` remain ISO-8601 instants.
- At Jakarta midnight, “Today” displays `—` until the first event of the new Jakarta day.
- Cache only the latest Level 2 envelope per valid `(metric, scope)` pair; do not reconstruct the 24-point window history.
- The cache is process-local and disappears when the Go process restarts.
- Replay Level 2 values only to dashboard-role WebSocket clients; keep Level 1 role filtering unchanged.
- Preserve exact raw bytes with defensive copies at the source, cache, and per-client boundaries.
- Do not add persistent storage, a Kafka replay API, a third-party frontend test framework, Flink high availability, or idempotent partial-job recovery.
- Keep `flink-job-submit` as a one-shot local container and document that partial submission may require manual Compose cleanup.
- Document that Flink aggregate state and the Go replay cache do not survive cluster/process recreation.

## File Structure

```text
flink/src/main/java/com/flinkdemo/level2/
├── DailyTime.java                         # Jakarta date-key and next-midnight conversion
├── MetricJob.java                         # Uses DailyTime for daily keying
└── function/DailyAggregateFunction.java   # Uses DailyTime for window_end
flink/src/test/java/com/flinkdemo/level2/
└── MetricFunctionsTest.java               # Jakarta boundary and aggregate tests
app/internal/ws/
├── hub.go                                 # Latest-value cache and dashboard replay
└── hub_test.go                            # Replay, role, malformed JSON, and copy tests
app/internal/kafkaclient/
└── consumer_test.go                       # Exact raw-byte forwarding assertion
web/src/lib/
├── jakartaDay.ts                          # Pure Jakarta date/boundary helpers
└── jakartaDay.test.ts                     # Node built-in tests, no new dependency
web/src/pages/
└── Dashboard.tsx                          # Jakarta clock and current-day filtering
web/package.json                           # Adds the built-in helper test command
Makefile                                   # Includes the helper test in verify
README.md                                  # Teaching-demo recovery limitations
```

---

### Task 1: Jakarta Daily Keys and Window Boundaries

**Files:**
- Create: `flink/src/main/java/com/flinkdemo/level2/DailyTime.java`
- Modify: `flink/src/main/java/com/flinkdemo/level2/MetricJob.java`
- Modify: `flink/src/main/java/com/flinkdemo/level2/function/DailyAggregateFunction.java`
- Test: `flink/src/test/java/com/flinkdemo/level2/MetricFunctionsTest.java`

**Interfaces:**
- Produces: `DailyTime.dateKey(String timestamp): String`
- Produces: `DailyTime.windowEnd(String dateKey): String`
- `MetricJob.build` uses `DailyTime.dateKey` before `DailyAggregateFunction`.
- `DailyAggregateFunction` uses `DailyTime.windowEnd` for each emitted daily envelope.

- [ ] **Step 1: Write failing Jakarta boundary tests**

Add the import:

```java
import static org.junit.jupiter.api.Assertions.assertNotEquals;
```

Add these tests to `MetricFunctionsTest`:

```java
@Test void dailyTimeUsesJakartaCalendarBoundary() {
    assertEquals("2026-07-25", DailyTime.dateKey("2026-07-25T16:59:59Z"));
    assertEquals("2026-07-26", DailyTime.dateKey("2026-07-25T17:00:00Z"));
    assertNotEquals(
        DailyTime.dateKey("2026-07-25T16:59:59Z"),
        DailyTime.dateKey("2026-07-25T17:00:00Z"));
    assertEquals("2026-07-25T17:00:00Z", DailyTime.windowEnd("2026-07-25"));
    assertEquals("2026-07-26T17:00:00Z", DailyTime.windowEnd("2026-07-26"));
}

@Test void dailyAggregateKeepsIndependentJakartaDayCounts() throws Exception {
    List<WindowStat> output = runDaily("tx_count", false, List.of(
        event("e1", "cart.checkout", "2026-07-25T16:00:00Z", "{}"),
        event("e2", "cart.checkout", "2026-07-25T16:59:59Z", "{}"),
        event("e3", "cart.checkout", "2026-07-25T17:00:00Z", "{}")));

    assertEquals(
        List.of(1L, 2L, 1L),
        output.stream().map(WindowStat::getValue).collect(Collectors.toList()));
    assertEquals(
        List.of(
            "2026-07-25T17:00:00Z",
            "2026-07-25T17:00:00Z",
            "2026-07-26T17:00:00Z"),
        output.stream().map(WindowStat::getWindowEnd).collect(Collectors.toList()));
}
```

Replace the `runDaily` key selector with:

```java
.keyBy(event -> DailyTime.dateKey(event.getTimestamp()))
```

Remove the superseded `dailyAggregateKeepsIndependentUtcDayCounts` test and its unused `LocalDate` import.

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
mvn -f flink/pom.xml -Dtest=MetricFunctionsTest test
```

Expected: test compilation fails because `DailyTime` does not exist.

- [ ] **Step 3: Implement `DailyTime`**

Create `flink/src/main/java/com/flinkdemo/level2/DailyTime.java`:

```java
package com.flinkdemo.level2;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

public final class DailyTime {
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");

    private DailyTime() {}

    public static String dateKey(String timestamp) {
        return Instant.parse(timestamp).atZone(JAKARTA).toLocalDate().toString();
    }

    public static String windowEnd(String dateKey) {
        return LocalDate.parse(dateKey)
            .plusDays(1)
            .atStartOfDay(JAKARTA)
            .toInstant()
            .toString();
    }
}
```

- [ ] **Step 4: Wire Jakarta semantics into the pipeline**

In `MetricJob.java`, remove `Instant` and `ZoneOffset` imports and replace the daily key selector with:

```java
.keyBy(event -> DailyTime.dateKey(event.getTimestamp()))
```

In `DailyAggregateFunction.java`, import `com.flinkdemo.level2.DailyTime`, remove `LocalDate` and `ZoneOffset`, and replace the `windowEnd` assignment with:

```java
String windowEnd = DailyTime.windowEnd(context.getCurrentKey());
```

- [ ] **Step 5: Run focused and full Flink tests**

Run:

```bash
mvn -f flink/pom.xml -Dtest=MetricFunctionsTest test
mvn -f flink/pom.xml clean verify
```

Expected:

```text
MetricFunctionsTest: 8 tests, 0 failures, 0 errors
All Flink tests: 11 tests, 0 failures, 0 errors
BUILD SUCCESS
```

- [ ] **Step 6: Commit**

```bash
git add flink/src/main/java/com/flinkdemo/level2/DailyTime.java \
  flink/src/main/java/com/flinkdemo/level2/MetricJob.java \
  flink/src/main/java/com/flinkdemo/level2/function/DailyAggregateFunction.java \
  flink/src/test/java/com/flinkdemo/level2/MetricFunctionsTest.java
git commit -m "fix: use Jakarta days for daily Flink metrics"
```

---

### Task 2: Process-Local Latest Metric Replay

**Files:**
- Modify: `app/internal/ws/hub.go`
- Test: `app/internal/ws/hub_test.go`
- Test: `app/internal/kafkaclient/consumer_test.go`

**Interfaces:**
- `Hub.metricCache` stores one owned `[]byte` for each allowed `metric + "\x00" + scope` key.
- `metricCacheKey([]byte): (string, bool)` accepts only the ten planned metric/scope combinations.
- `Hub.Run` replays cached values in sorted key order to a registering dashboard client.
- `BroadcastRaw([]byte)` retains its non-blocking enqueue interface and source-copy guarantee.

- [ ] **Step 1: Write failing replay and copy-isolation tests**

Add these imports to `hub_test.go`:

```go
import (
    "bytes"
    "testing"
    "time"

    "github.com/kuang/flink-demo/internal/event"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)
```

Add this helper:

```go
func waitForRawDrain(t *testing.T, hub *Hub) {
    t.Helper()
    require.Eventually(t, func() bool {
        return len(hub.raw) == 0
    }, time.Second, 10*time.Millisecond)
}
```

Add these tests:

```go
func TestDashboardRegistrationReplaysLatestMetricPerScope(t *testing.T) {
    hub := NewHub()
    go hub.Run()
    defer hub.Close()

    first := []byte(`{"metric":"tx_count","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":1,"detail":{}}`)
    latest := []byte("{\n  \"metric\":\"tx_count\",\"scope\":\"daily\",\"window_end\":\"2026-07-25T17:00:00Z\",\"value\":2,\"detail\":{}\n}")
    hub.BroadcastRaw(first)
    waitForRawDrain(t, hub)
    hub.BroadcastRaw(latest)
    waitForRawDrain(t, hub)

    dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 10)}
    hub.Register <- dashboard

    require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
    assert.True(t, bytes.Equal(latest, <-dashboard.send))
}

func TestMetricReplayIsDashboardOnlyAndOwnsEveryBuffer(t *testing.T) {
    hub := NewHub()
    go hub.Run()
    defer hub.Close()

    source := []byte(`{"metric":"revenue","scope":"daily","window_end":"2026-07-25T17:00:00Z","value":489000,"detail":{}}`)
    expected := append([]byte(nil), source...)
    hub.BroadcastRaw(source)
    source[0] = '!'
    waitForRawDrain(t, hub)

    first := &Client{Name: "dash-1", Role: "dashboard", send: make(chan []byte, 10)}
    second := &Client{Name: "dash-2", Role: "dashboard", send: make(chan []byte, 10)}
    buyer := &Client{Name: "buyer", Role: "buyer", send: make(chan []byte, 10)}
    hub.Register <- first
    hub.Register <- second
    hub.Register <- buyer

    require.Eventually(t, func() bool {
        return len(first.send) == 1 && len(second.send) == 1
    }, time.Second, 10*time.Millisecond)
    firstMessage := <-first.send
    secondMessage := <-second.send
    assert.True(t, bytes.Equal(expected, firstMessage))
    assert.True(t, bytes.Equal(expected, secondMessage))
    firstMessage[0] = '!'
    assert.True(t, bytes.Equal(expected, secondMessage))
    assert.Never(t, func() bool { return len(buyer.send) != 0 }, 100*time.Millisecond, 10*time.Millisecond)
}

func TestMalformedRawMessageIsNotCached(t *testing.T) {
    hub := NewHub()
    go hub.Run()
    defer hub.Close()

    hub.BroadcastRaw([]byte(`{"metric":`))
    waitForRawDrain(t, hub)

    dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 10)}
    hub.Register <- dashboard
    assert.Never(t, func() bool { return len(dashboard.send) != 0 }, 100*time.Millisecond, 10*time.Millisecond)

    valid := []byte(`{"metric":"tx_count","scope":"window","window_end":"2026-07-25T10:00:00Z","value":3,"detail":{}}`)
    hub.BroadcastRaw(valid)
    require.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
    assert.True(t, bytes.Equal(valid, <-dashboard.send))
}
```

In `consumer_test.go`, import `bytes`, replace the Flink input with deliberately formatted JSON, and replace `assert.JSONEq` with exact comparison:

```go
raw := []byte("{\n \"metric\":\"tx_count\", \"scope\":\"window\", \"window_end\":\"2026-07-18T10:05:00Z\", \"value\":7, \"detail\":{}\n}")
require.NoError(t, consumer.forward("flink.window.stats", raw))
assert.True(t, bytes.Equal(raw, recorder.raw[0]))
```

- [ ] **Step 2: Run focused Go tests to verify they fail**

Run:

```bash
cd app && go test ./internal/ws ./internal/kafkaclient
```

Expected: replay tests fail because registering dashboards receive no previously broadcast metric.

- [ ] **Step 3: Add the bounded metric cache**

In `hub.go`, add `sort` to imports and add:

```go
var allowedMetricScopes = map[string]map[string]bool{
    "listings_count":   {"window": true},
    "cart_adds_count":  {"window": true},
    "tx_count":         {"window": true, "daily": true},
    "confirmed_orders": {"window": true},
    "delivered_orders": {"window": true, "daily": true},
    "top_product":      {"window": true},
    "revenue":          {"daily": true},
}

type metricIdentity struct {
    Metric string `json:"metric"`
    Scope  string `json:"scope"`
}

func metricCacheKey(data []byte) (string, bool) {
    var identity metricIdentity
    if err := json.Unmarshal(data, &identity); err != nil {
        return "", false
    }
    scopes, ok := allowedMetricScopes[identity.Metric]
    if !ok || !scopes[identity.Scope] {
        return "", false
    }
    return identity.Metric + "\x00" + identity.Scope, true
}
```

Add the cache field:

```go
metricCache map[string][]byte
```

Initialize it in `NewHub`:

```go
metricCache: make(map[string][]byte),
```

- [ ] **Step 4: Replay cache entries during dashboard registration**

After adding a client to `h.clients` in the register case, add:

```go
if client.Role == "dashboard" {
    keys := make([]string, 0, len(h.metricCache))
    for key := range h.metricCache {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    for _, key := range keys {
        message := append([]byte(nil), h.metricCache[key]...)
        select {
        case client.send <- message:
        default:
            slog.Warn("dashboard replay buffer full", "name", client.Name)
        }
    }
}
```

In the raw case, replace `h.mu.RLock()` with `h.mu.Lock()`, replace the matching
`h.mu.RUnlock()` with `h.mu.Unlock()`, and add this before iterating clients:

```go
if key, ok := metricCacheKey(data); ok {
    h.metricCache[key] = append([]byte(nil), data...)
} else {
    slog.Warn("raw Level 2 message is not cacheable")
}
```

Keep the existing per-client `append([]byte(nil), data...)`.

- [ ] **Step 5: Run focused and full Go tests**

Run:

```bash
cd app && go test ./internal/ws ./internal/kafkaclient
cd app && go test ./...
```

Expected: all focused and full Go tests pass.

- [ ] **Step 6: Commit**

```bash
git add app/internal/ws/hub.go app/internal/ws/hub_test.go app/internal/kafkaclient/consumer_test.go
git commit -m "feat: replay latest Level 2 metrics to dashboards"
```

---

### Task 3: Jakarta Midnight Dashboard Filtering

**Files:**
- Create: `web/src/lib/jakartaDay.ts`
- Create: `web/src/lib/jakartaDay.test.ts`
- Modify: `web/src/pages/Dashboard.tsx`
- Modify: `web/package.json`
- Modify: `Makefile`
- Regenerate: `app/web/dist`

**Interfaces:**
- Produces: `jakartaDateKey(Date): string`
- Produces: `jakartaDayForWindowEnd(String): string | null`
- Produces: `millisecondsUntilNextJakartaMidnight(Date): number`
- Dashboard state keeps a current Jakarta date key and filters daily records against it.

- [ ] **Step 1: Add the failing built-in Node tests**

Create `web/src/lib/jakartaDay.test.ts`:

```ts
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  jakartaDateKey,
  jakartaDayForWindowEnd,
  millisecondsUntilNextJakartaMidnight,
} from './jakartaDay.ts';

test('Jakarta date changes at 17:00 UTC', () => {
  assert.equal(jakartaDateKey(new Date('2026-07-25T16:59:59Z')), '2026-07-25');
  assert.equal(jakartaDateKey(new Date('2026-07-25T17:00:00Z')), '2026-07-26');
});

test('daily window belongs to the Jakarta day immediately before its end', () => {
  assert.equal(jakartaDayForWindowEnd('2026-07-25T17:00:00Z'), '2026-07-25');
  assert.equal(jakartaDayForWindowEnd('not-a-date'), null);
});

test('next Jakarta midnight delay reaches the 17:00 UTC boundary', () => {
  assert.equal(
    millisecondsUntilNextJakartaMidnight(new Date('2026-07-25T16:59:59Z')),
    1000,
  );
});
```

Add this script to `web/package.json`:

```json
"test:jakarta": "node --test src/lib/jakartaDay.test.ts"
```

- [ ] **Step 2: Run the helper test to verify it fails**

Run:

```bash
cd web && npm run test:jakarta
```

Expected: FAIL because `jakartaDay.ts` does not exist.

- [ ] **Step 3: Implement the pure Jakarta date helpers**

Create `web/src/lib/jakartaDay.ts`:

```ts
const JAKARTA_OFFSET_MS = 7 * 60 * 60 * 1000;

const jakartaFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Jakarta',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

function jakartaParts(date: Date): { year: number; month: number; day: number } {
  const values = Object.fromEntries(
    jakartaFormatter
      .formatToParts(date)
      .filter(({ type }) => type === 'year' || type === 'month' || type === 'day')
      .map(({ type, value }) => [type, Number(value)]),
  );
  return { year: values.year, month: values.month, day: values.day };
}

export function jakartaDateKey(date: Date): string {
  const { year, month, day } = jakartaParts(date);
  return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

export function jakartaDayForWindowEnd(windowEnd: string): string | null {
  const end = Date.parse(windowEnd);
  if (!Number.isFinite(end)) return null;
  return jakartaDateKey(new Date(end - 1));
}

export function millisecondsUntilNextJakartaMidnight(now: Date): number {
  const { year, month, day } = jakartaParts(now);
  const nextMidnight = Date.UTC(year, month - 1, day + 1) - JAKARTA_OFFSET_MS;
  return Math.max(1, nextMidnight - now.getTime());
}
```

- [ ] **Step 4: Verify the helper tests pass**

Run:

```bash
cd web && npm run test:jakarta
```

Expected: 3 tests pass, 0 fail.

- [ ] **Step 5: Filter daily values with a Jakarta-day clock**

In `Dashboard.tsx`, import:

```ts
import {
  jakartaDateKey,
  jakartaDayForWindowEnd,
  millisecondsUntilNextJakartaMidnight,
} from '../lib/jakartaDay';
```

Add state inside `Dashboard`:

```ts
const [jakartaDay, setJakartaDay] = useState(() => jakartaDateKey(new Date()));
```

Add this effect:

```ts
useEffect(() => {
  let timer: ReturnType<typeof setTimeout>;
  const scheduleMidnightRefresh = () => {
    timer = setTimeout(() => {
      setJakartaDay(jakartaDateKey(new Date()));
      scheduleMidnightRefresh();
    }, millisecondsUntilNextJakartaMidnight(new Date()) + 50);
  };
  scheduleMidnightRefresh();
  return () => clearTimeout(timer);
}, []);
```

Replace the daily selection with:

```ts
const daily = values.find(
  (item) =>
    item.scope === 'daily' &&
    jakartaDayForWindowEnd(item.window_end) === jakartaDay,
);
```

Replace the Level 2 explanatory text with:

```tsx
<p>Five-minute windows slide every five seconds; daily totals reset at Jakarta midnight (WIB).</p>
```

- [ ] **Step 6: Add the helper test to the full gate and regenerate assets**

In `Makefile`, add this line before lint:

```make
	cd web && npm run test:jakarta
```

Run:

```bash
cd web && npm run test:jakarta
cd web && npm run lint
cd web && npm run build
```

Expected:

```text
3 Node tests pass
oxlint exits 0 with only the three existing Fast Refresh warnings
Vite build succeeds and regenerates app/web/dist
```

- [ ] **Step 7: Commit**

```bash
git add web/src/lib/jakartaDay.ts web/src/lib/jakartaDay.test.ts \
  web/src/pages/Dashboard.tsx web/package.json Makefile app/web/dist
git commit -m "fix: reset dashboard daily values at Jakarta midnight"
```

---

### Task 4: Demo Limitations and Final Verification

**Files:**
- Modify: `README.md`
- Modify only other files required to fix a failing verification; do not broaden scope.

**Interfaces:**
- Documents the operational boundary already approved by the user.
- Produces a reproducible green full verification and live demo check.

- [ ] **Step 1: Document accepted limitations**

Append to `README.md`:

```markdown
## Teaching-demo limitations

- Daily Level 2 metrics use Jakarta calendar days and reset at Jakarta midnight (WIB).
- The Go service keeps only the latest Level 2 value per metric/scope in memory. Reloaded dashboards recover current values, not full chart history, and the cache is lost when the app restarts.
- `flink-job-submit` is a one-shot local helper. If submission is interrupted partway through, run `docker compose down -v` before retrying the demo.
- Flink aggregate state is not restored after the Compose cluster is recreated. This repository demonstrates live stream processing, not production-grade high availability or disaster recovery.
```

- [ ] **Step 2: Run all local quality gates**

Run:

```bash
make verify
```

Expected:

```text
Go tests pass
3 Jakarta helper tests pass
oxlint exits 0 with only the three existing Fast Refresh warnings
Vite build succeeds
Maven reports 11 tests, 0 failures, 0 errors, and BUILD SUCCESS
```

- [ ] **Step 3: Rebuild and run the full Compose smoke test**

Run:

```bash
docker compose down -v
PATH="/d/CodexTools/jq:$PATH" ./scripts/phase3-smoke.sh
```

Expected:

```text
Phase 3 smoke test passed: seven jobs running and Level 2 metrics observed.
```

- [ ] **Step 4: Verify replay and Jakarta rendering in the live dashboard**

Open `http://localhost:15300/dashboard`, wait for current values, then reload the page without creating another transaction.

Verify:

```text
The dashboard reconnects.
Latest Level 2 window and daily values reappear from the Go cache.
The explanatory text says Jakarta midnight (WIB).
Revenue remains an integer IDR value with no fractional digits.
Window-only Today values remain an em dash.
Browser console and WebSocket frames contain no errors.
Flink Web UI still shows exactly seven RUNNING jobs and one TaskManager.
```

For the midnight rule, run the pure helper test from Step 2; do not change the host clock.

- [ ] **Step 5: Confirm packaging and source constraints**

Run:

```bash
docker compose config --quiet
test -f flink/target/level2-jobs.jar
git diff -- web/package-lock.json
grep -R "\/ 100\|\* 100" app web/src flink/src/main || true
git diff --check
git restore --worktree -- flink/target
git clean -fd -- flink/target
git status --short
```

Expected:

```text
Compose model and JAR checks pass.
No package-lock diff exists because no dependency was added.
The currency scan prints nothing.
No whitespace errors exist.
Only Maven-generated files under flink/target are restored or removed.
Only the README change is uncommitted.
```

- [ ] **Step 6: Commit documentation**

```bash
git add README.md
git commit -m "docs: record teaching demo recovery limits"
```

- [ ] **Step 7: Confirm the final worktree is clean**

Run:

```bash
git status --short
```

Expected: no output.

---

## Final Review Checklist

- Jakarta daily date keys change at `17:00:00Z`.
- Daily `window_end` is the next Jakarta midnight expressed as an instant.
- Yesterday's daily replay renders as `—` after Jakarta midnight.
- Go stores at most one latest value per allowed metric/scope.
- Cache replay is dashboard-only, deterministic, and byte-copy isolated.
- No third-party frontend test dependency is added.
- One-shot submission and non-durable cluster recovery remain unchanged and are documented.
- Full local, Compose, smoke, reload, and browser-console gates pass.
