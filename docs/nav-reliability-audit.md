# NAV reliability audit — baseline `16e9fe7`

Read-only audit on 2026-11-11. These are baseline facts, not descriptions of the
new implementation. No existing database was queried or modified for this audit.

## Confirmed control flow

- `services/ingestion.py:14-36`: CLI, worker and Airflow call the same service.
  One run is inserted, the entire HTTP body is fetched, parsed in a thread, then
  written in unordered batches of 1,000 upserts. There are no service retries.
- `ingest.py:14-22`: CLI calls it directly. `pipelines/nav_daily.py:9-23` calls the
  CLI coroutine; weekdays 19:00 **UTC**, catchup disabled, max active DAG runs one,
  two retries with a five-minute delay. This does not serialize other callers.
- `worker.py:21-81`: queued or expired jobs are claimed, a heartbeat runs every
  25 seconds, leases last 120 seconds; exceptions requeue up to three attempts.
  `storage.py:63-72` atomically claims a job, but checks before later writes are
  separate operations. NAV bulk writes do not check job ownership at all.
- `api/workbench.py:243-264`: authenticated users queue one active NAV job;
  ingestion runs and public-source history are shared. This queue's unique index
  cannot coordinate the CLI or Airflow. API lookups use a separate in-memory
  `NavCache`, not persisted history (`api/app.py:37,107-109`).

## What already works

- `storage.py:28-40` creates unique `(scheme_code, date)` and `(job_id,row_number)`
  indexes, user uniqueness, session TTL, job claim indexes, and a partial unique
  active NAV job index. NAV upserts avoid duplicate observations on replay.
- `services/nav.py:16-41` supports six/eight-column feeds, decimal NAV values,
  finite/nonnegative checks, date normalization and numeric-row rejections. No
  valid rows raises before writes. Scheme dates differ from download timestamps.
- Source parsing, lookup-cache concurrency and existing API contracts have offline
  tests. `scripts/check_backend.py:78-103` checks real Mongo claim/recovery and
  two synthetic source replays in a uniquely named temporary database.

## Gaps, in priority order

1. **Concurrency safety:** a lease token is a compare-and-set for that document,
   not a fence on other documents. It may expire between check and write. An old
   writer can continue after another caller starts. The Compose Mongo 7 service
   has no replica-set configuration; multi-document transactions are unavailable
   on that configured topology. Runtime production topology has not been assumed.
2. **Reproducibility:** the 15 MB limit is checked after full download; no source
   hash, byte count, retained artifact or fixture CLI. The current feed cannot
   backfill historical dates without saved historical files.
3. **Metadata:** baseline run fields are `_id`, `created_at`, `status`, `source`,
   `job_id`, `accepted`, `rejected_count`, capped `rejected_rows`, `finished_at`,
   `updated_at`, and generic `error`. `accepted` counts parsed source rows, never
   new Mongo documents. Bulk result counts, duration and source date range are
   absent. Partial writes and uncertain network outcomes are not distinguished.
4. **Recovery:** only retries of the same worker job mark previous runs
   interrupted. CLI/Airflow crashes can leave `running` forever. Worker completion
   and ingestion completion are separate updates. Lease renewal ignores expiry.
5. **Observability/testing:** no ingestion summary metrics or worker heartbeat;
   faults, simultaneous entry points and stale-writer races lack coverage. The
   integration script creates synthetic accounts on its configured backend; run
   the upgraded checks on an isolated backend, never a production database.

## Incremental plan and decisions

1. Stream bounded sources; SHA-256 and bounded fixture replay; configurable
   quality checks; additive run metadata and exact acknowledged write counts.
   Preserve legacy response fields and the unique NAV index. Test offline first.
2. Choose coordination explicitly: verified replica-set transactions can couple a
   lease compare-and-set and each NAV batch/run update; standalone cannot provide
   that guarantee. Keep the existing data volume untouched. Validate the chosen
   design using isolated Mongo and adversarial schedules, including disconnects.
3. Add authenticated metrics, dashboard summaries, run/job reconciliation,
   deployment/rollback notes, architecture diagram and reproducible test commands.
   Test and commit each milestone; do not stage unrelated local changes.

Reference: [MongoDB atomicity and transactions](https://www.mongodb.com/docs/manual/core/transactions/),
[HTTPX async streaming](https://www.python-httpx.org/async/).
