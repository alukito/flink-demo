# Jakarta Daily Metrics and Dashboard Replay Design

**Date:** 2026-07-25

**Status:** Approved for implementation planning

## Goal

Make the Phase 3 teaching dashboard use Jakarta calendar days for daily metrics and restore the latest Level 2 values when a dashboard reconnects, while keeping the demo deliberately local and non-durable.

## Scope

This change will:

- derive daily Flink keys from event timestamps in `Asia/Jakarta`;
- represent each daily `window_end` as the instant of the next Jakarta midnight;
- clear stale daily values from the dashboard at Jakarta midnight, showing an em dash until the first event of the new day;
- cache and replay the latest Level 2 envelope for each `(metric, scope)` pair in the Go process;
- preserve dashboard-only delivery of Level 2 metrics and all existing Level 1 role filtering; and
- document accepted demo limitations around one-shot job submission and cluster recreation.

This change will not add persistent storage, Kafka replay APIs, a frontend test framework, Flink high availability, or idempotent recovery for partially submitted jobs.

## Daily-Time Semantics

Event timestamps remain UTC ISO-8601 instants at the system boundary. Flink converts each event timestamp with the IANA zone `Asia/Jakarta` and uses the resulting Jakarta `LocalDate` as the daily state key.

For a Jakarta date, the daily envelope's `window_end` is the next Jakarta midnight converted back to an ISO-8601 instant. For example:

- `2026-07-25T16:59:59Z` belongs to 25 July in Jakarta and ends at `2026-07-25T17:00:00Z`;
- `2026-07-25T17:00:00Z` belongs to 26 July in Jakarta and ends at `2026-07-26T17:00:00Z`.

The dashboard treats a daily record as current only when the Jakarta date immediately before its `window_end` equals the current Jakarta date. It schedules a state refresh at the next Jakarta midnight. After that refresh, yesterday's cached daily values are ignored and “Today” displays `—` until a new event produces a current-day record.

The teaching text will say that daily totals reset at Jakarta midnight (WIB), not UTC midnight.

## In-Memory Metric Replay

The WebSocket hub will maintain a process-local cache keyed by `(metric, scope)`. The maximum expected cache is ten envelopes: seven window values and three daily values.

When `BroadcastRaw` receives a Level 2 envelope:

1. it keeps the existing defensive byte copy;
2. the hub event loop broadcasts it only to dashboard-role clients;
3. if it is a recognized Level 2 envelope with a non-empty metric and scope, the event loop replaces that cache entry with another defensive copy; and
4. malformed or unrelated JSON remains eligible for the existing live raw delivery path but is not cached.

When a dashboard client registers, the hub enqueues one defensive copy of every cached value to that client before later live values. Non-dashboard clients receive no cached Level 2 data. Replay ordering will be deterministic by metric and scope so tests and demonstrations are reproducible.

The frontend's existing `(metric, scope, window_end)` replacement logic consumes replayed values without a new protocol. Only the latest value per pair is restored; the prior 24-point chart history is not reconstructed after a reload.

The cache intentionally disappears when the Go process restarts.

## Error Handling

- A Level 2 message that cannot be decoded for a cache key is still handled by the existing live raw-broadcast path and produces a warning rather than terminating the consumer.
- Every cached and replayed byte slice is copied so Kafka-buffer reuse or one WebSocket client cannot mutate another client's message.
- A full client send buffer follows the existing non-blocking behavior and drops that client's replay/live message rather than blocking the hub.
- Jakarta date parsing failures continue through the existing Flink job failure and restart behavior; no silent fallback to UTC is introduced.

## Testing and Verification

### Flink

- Add failing tests for events immediately before and at `17:00:00Z`.
- Verify the two events use different Jakarta daily keys.
- Verify the emitted `window_end` values are the corresponding next Jakarta midnights.
- Preserve integer revenue and existing daily accumulation tests.

### Go

- Add failing hub tests proving the latest value replaces an earlier value for the same `(metric, scope)`.
- Verify a newly registered dashboard receives cached values.
- Verify non-dashboard roles receive no replay.
- Verify exact byte equality and source/client buffer isolation for raw messages.
- Verify malformed JSON is not cached and does not stop later valid replay.

### Frontend and Integration

- Keep the no-new-test-framework constraint.
- Run frontend lint and build gates.
- Use the live/headless dashboard check to confirm Jakarta wording, current-day values, and stale daily values rendering as `—`.
- Run the complete Go, frontend, Maven, Compose, and Phase 3 smoke verification afterward.

## Accepted Demo Limitations

- `flink-job-submit` remains a one-shot local submission container. A partial local failure can require manual Compose cleanup before retrying.
- Flink checkpoints and the Go replay cache are not durable across cluster/process recreation. Recreating the stack can lose aggregation and replay state.
- These limitations are acceptable for the supervised afternoon teaching demo and must not be represented as production-grade recovery.
