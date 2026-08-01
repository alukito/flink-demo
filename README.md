# flink-demo
Demo of e-commerce system using flink

## Teaching-demo limitations

- Daily Level 2 metrics use Jakarta calendar days and reset at Jakarta midnight (WIB).
- The Go service keeps only the latest Level 2 value per metric/scope in memory. Reloaded dashboards recover current values, not full chart history, and the cache is lost when the app restarts.
- `flink-job-submit` is a one-shot local helper. If submission is interrupted partway through, run `docker compose down -v` before retrying the demo.
- Flink aggregate state is not restored after the Compose cluster is recreated. This repository demonstrates live stream processing, not production-grade high availability or disaster recovery.
