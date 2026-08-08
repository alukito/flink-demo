# flink-demo
Demo of e-commerce system using flink

## Teaching-demo limitations

- Daily Level 2 metrics use Jakarta calendar days and reset at Jakarta midnight (WIB).
- The Go service keeps only the latest Level 2 value per metric/scope in memory. Reloaded dashboards recover current values, not full chart history, and the cache is lost when the app restarts.
- Level 3 CEP alerts are immutable observations: the dashboard derives counts from alert IDs and does not support acknowledging, editing, or deleting alerts. The Go replay cache is process-local and retains only the last eight hours, so a browser reload can replay that window but cannot recover older alerts or alerts from before an app restart.
- `flink-job-submit` is a one-shot local helper. If submission is interrupted partway through, run `docker compose down -v` before retrying the demo.
- Flink aggregate state is not restored after the Compose cluster is recreated. This repository demonstrates live stream processing, not production-grade high availability or disaster recovery.

## Level 3 CEP demo

Start a fresh demo with `docker compose down -v` and then run
`./scripts/phase4-smoke.sh` (requires `docker`, `curl`, and `jq`). The script
starts the stack when needed, confirms twelve RUNNING Flink jobs, and leaves a
successful stack running for dashboard inspection. One TaskManager is sized
with 12 slots and 2048m process memory for the seven Level 2 and five Level 3
jobs.

The smoke flow demonstrates five actions and their immutable alerts:

1. Add an item to a cart and leave it without checkout for two event-time minutes (`abandoned_cart`).
2. Have three distinct buyers add the same product within one event-time minute (`trending_product`).
3. Have three distinct buyers check out within 30 event-time seconds (`order_surge`).
4. Check out an order, confirm and pick it, then deliver it (`delivery_completed`).
5. Pick a confirmed order and leave it undelivered for one event-time minute (`slow_delivery`).

The dashboard counts alerts from this output rather than changing their state.
Hovering the Level 3 chart points shows their timestamp and value. Browser and
Go replay is intentionally limited to the most recent eight hours.
