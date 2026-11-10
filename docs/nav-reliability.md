# NAV ingestion reliability

The initial, code-backed baseline audit is in [nav-reliability-audit.md](nav-reliability-audit.md).
The implementation uses MongoDB replica-set transactions. **Standalone MongoDB
fails closed for NAV ingestion** with `error_category=topology`; the other backend
features and original MCP tools still work. No existing volume has been migrated.

## Coordination and the fencing argument

Every CLI, Airflow and Mongo-worker ingestion calls `services/ingestion.py`.
`nav_coordination` contains one document, `_id=amfi_nav`, with an owner run ID,
monotonically increasing version, server-time expiry, heartbeat and reclaim count.
Acquisition atomically claims an idle/expired lease and increments its version.
It also reconciles older running attempts in the same transaction. Expiry uses
MongoDB `$$NOW`, not the caller's clock. Leases renew during slow downloads and
before each batch. Claims and batches use majority write concern.

Each batch transaction first **writes** the coordination document using owner,
version and unexpired-lease predicates, then performs unordered NAV upserts and
increments that run's result counters. A worker ingestion also writes its exact
unexpired job lease token within this transaction. Heartbeats and batches on the
same run serialize their sessions; driver retry callbacks have no Python-counter,
HTTP or filesystem side effects. Driver CSOT bounds a transaction to 20 seconds.

Suppose a newer owner has committed an acquisition. An old batch either started
later and fails its owner/version predicate, or started on an earlier snapshot
and conflicts when writing the changed coordination document. Its entire batch
aborts. If an old transaction has already locked that document, acquisition must
wait/conflict until that transaction commits or aborts. Therefore an old batch
cannot commit **after a newer ownership grant**. Lease expiry allows a takeover;
it is not a wall-clock guarantee that a previously ordered transaction is rolled
back at that exact instant. Tests exercise both transaction orderings.

Completing a run requires the same fence. Worker job completion and run completion
commit together. A periodic worker reconciler fences an expired owner before
marking its run interrupted. Terminal metadata is never blindly overwritten by a
late old exception. Expired worker tokens cannot be resurrected. This NAV fencing
does not extend to the existing CSV row workflow or arbitrary direct DB clients.

## Atomicity, replay and errors

Atomicity is **per batch**, not per source file. If batch two fails, batch one and
its acknowledged counts remain. A bulk error aborts all operations in its current
transaction, including operations MongoDB processed before the error. Retrying a
source uses the existing unique `(scheme_code,date)` index and bulk upserts. It
does not append duplicate observations or skip a source solely because its hash
was seen before. Later data for the same scheme/date can replace earlier data.

Run fields are additive. `_id` remains unique; `run_id` aliases it. `created_at`,
`status`, `source`, `job_id`, `accepted`, `rejected_count`, `rejected_rows` and
`finished_at` retain their meanings. New fields record `trigger`, `started_at`,
`duration_seconds`, source SHA-256/bytes/completeness, numeric candidate count
(`parsed_count`), source date range, effective quality rules, exact `matched_count`,
`upserted_count`, `modified_count`, batches committed, version, and sanitized
`error_category`/`diagnostic`. Headers/category lines are not parsed candidates.
Duplicates after the first scheme/date are rejected deterministically.

`accepted` is a source-row count. `upserted_count` is newly inserted observations;
`matched_count` is existing observations. `modified_count` includes provenance and
ingestion timestamp updates, so an identical replay can modify documents without
changing their NAV value. Counters commit with their writes and are not accumulated
in retried Python callbacks. Missing counters on legacy runs mean unknown, not zero.

Categories include `source_http`, `source_timeout`, `source_network`, `source_size`,
`source_encoding`, `source_file`, `parse`, `quality`, `artifact`, `write`, `database`,
`coordination_busy`, `lease_lost`, `topology`, `configuration`, `interrupted` and
`internal`. Diagnostics never include exception strings, credentials, URLs with
parameters, source bodies or host paths. A complete source hash/size is saved even
if decoding or quality fails. Incomplete sources have null hash/size, not a hash
misrepresented as a complete source. Network ambiguity can leave a run `running`
until recovery/reconciliation; inspect persisted transactional counters.

Worker retries transient source/database/lease errors up to three attempts.
Contention defers for ten seconds without consuming a source attempt. Permanent
quality/parse/topology/size errors fail immediately. Airflow retains two retries
five minutes apart for transient errors and uses `AirflowFailException` for
permanent errors. Each attempt gets a distinct run ID; retries are at least once.
The CLI has no automatic source retry. Transaction retries are separate from
source-attempt retries and are bounded by the driver deadline.

## Source controls

HTTP downloads request identity encoding, reject compressed responses, and enforce
a byte limit as chunks arrive, independent of Content-Length. Complete entity
bytes determine SHA-256. Fixtures read at most limit + 1 bytes and use the same
parser, quality rules, coordination and writes as HTTP. UTF-8/BOM input is accepted.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `NAV_MAX_SOURCE_BYTES` | 15000000 | Maximum complete source bytes |
| `NAV_MIN_RECORDS` | 1 | Minimum accepted unique source rows |
| `NAV_MAX_REJECTED_FRACTION` | 1.0 | Rejected / numeric candidate rows; lower for stricter deployment |
| `NAV_MAX_SOURCE_AGE_DAYS` | unset | Maximum age of the oldest accepted source date |
| `NAV_MAX_FUTURE_DAYS` | unset | Maximum future offset of the newest source date |
| `NAV_RAW_DIR` | unset | Optional local directory for SHA-named raw artifacts |

Quality defaults preserve the original parser's acceptance policy. Choose stricter
thresholds after measuring a representative feed; funds can carry different NAV
dates. Metadata records the effective rules. `date` is the NAV source-record date,
`fetched_at` is complete source acquisition time, and `ingested_at` is the batch
write time. Local artifacts are optional; without retained historical files,
historical-source replay/backfill is impossible. Retention is not a backup system;
configure a persistent mount, disk budget and cleanup policy if enabling it.

## Monitoring endpoints and metric definitions

`GET /api/v1/ingestions/metrics` requires the existing bearer session. It reads
persisted operational state and returns JSON; no Prometheus server is required.
It is read-only and does not trigger recovery or make an upstream AMFI call.
`GET /api/v1/ingestion-runs` remains paginated detailed history. The existing NAV
dashboard displays health plus per-run new inserts, matches, duration and errors.

| JSON field | Definition |
| --- | --- |
| `observed_at` | Mongo server clock, normalized to UTC (command codecs can be naive) |
| `transaction_capable` | Verified replica-set/session/wire-version capabilities |
| `latest_attempt`, `last_completed`, `last_failure` | Latest attempt, successful terminal run, failed/interrupted terminal run; nullable before any such run |
| `last_completed.duration_seconds` | Invocation start to terminal completion, including acquisition/download/parsing/writes |
| `last_completed.accepted`, `rejected_count` | Unique valid source rows and rejected numeric candidates |
| `last_completed.upserted_count` | Newly inserted observations, from acknowledged transactional results |
| `last_completed.matched_count`, `modified_count` | Existing matches and modified observations, including provenance changes |
| `last_completed_age_seconds` | Server-clock seconds since last completed run |
| `source_freshness_days` | UTC calendar days since the latest source NAV date in the last completed run; negative for future data; not time since download |
| `consecutive_failures` | Failed/interrupted runs finishing after the last success; skipped contention attempts excluded |
| `run_totals` | Counts and sums per status across attempts, including replays; not unique dataset cardinality |
| `legacy_runs_without_write_counts` | Legacy runs with unknown write counts; never infer inserts from accepted rows |
| `jobs` | Global counts of queued/running/failed/completed/completed-with-errors jobs; no private row/user data |
| `job_lease_expired` | Running jobs whose server-time lease has expired |
| `job_lease_reclaims` | Sum of persisted per-job expired-lease reclaims |
| `workers.active` | Worker heartbeats within 30 seconds; presence renews every ten seconds |
| `workers.last_heartbeat_at` | Latest persisted worker heartbeat; nullable |
| `lease.owner_run_id`, `version`, `expires_at`, `expired` | Current NAV coordination state, without worker lease tokens |
| `lease.reclaims` | Expired-owner takeovers/invalidation by acquisition or reconciliation |

For monitoring, poll this endpoint with a session and alert on repeated failures,
no recent successful run, stale NAV source dates, queued work without an active
worker, or expired leases. Freshness thresholds must allow AMFI weekends/holidays;
an old source date is different from a worker that has stopped. A single-node
replica set enables transactions but provides no database high availability.

## Deployment and rollback

No deployment, conversion, volume removal or production-data operation is part of
this change. Do not run old unfenced ingestion code beside the new version.
Stop old worker/CLI writers and pause the DAG before an actual upgrade.

For a **fresh, separate local deployment**, use unused ports and a new project:

```bash
API_PORT=8011 AIRFLOW_PORT=8081 docker compose -p bharatpay-nav-demo \
  -f compose.yaml -f compose.nav-replica.yaml up --build -d --wait
```

The override creates `nav-rs-data`, not the existing `mongo-data` mount. Its
one-shot `mongo-init` initializes `bharatpay-rs` and waits for a primary. Application
services depend on that step and use a replica-set URI. Existing standalone
volumes stay intact. Do not use this command as an in-place data migration: the
new database is empty. A migration of existing records needs a separately planned
backup, restore/verification and switch of all clients to the same replica set.
For production, use an authenticated replica set with TLS and appropriate members;
the unauthenticated single-node example remains a local portfolio setup.

Validate `/ready`, authenticate, inspect `/api/v1/ingestions/metrics` for
`transaction_capable=true`, and perform synthetic fixture replay in a test
database before enabling a live schedule. To start Airflow, use the same project
and both compose files, add `--profile airflow`, and `up --build -d airflow`.
The DAG remains weekdays 19:00 UTC, catchup disabled, one active DAG run. Failed
permanent quality/topology checks do not consume Airflow transient retries.

Quality environment variables are shared by API-container CLI, worker and Airflow
through the base Compose environment anchor. Keep them consistent across external
CLI deployments too. Optional `NAV_RAW_DIR` also needs an explicit persistent
mount and permissions; it is deliberately not automatically enabled.

Rollback is a code rollout, not data deletion. Stop all ingestion writers/pause
the DAG first, restore the chosen prior image/commit, and retain both volumes.
Additive fields/indexes can remain; existing API consumers still see legacy
fields. Returning to old ingestion code also returns to its concurrency risks;
keep ingestion disabled or enforce one operator-controlled writer. Do not mix
old and new writers, drop collections, delete locks, reset versions, or delete
volumes to make recovery appear successful. Rolling back code does not undo
committed NAV changes; restore data only through a separately approved procedure.

## Validation

Offline tests never use MongoDB or live providers:

```bash
python -m pytest tests/ -q
```

With `mongod` installed and api/dev extras available, run the entire isolated flow:

```bash
python scripts/run_isolated_checks.py
```

This starts its own replica set, API, worker and standalone server using fresh
temporary paths/free ports. It invokes `scripts/check_backend.py`, the NAV fault
checks, explicit integration pytest, actual CLI fixture replay and actual
standalone refusal. Only its generated test databases/directories/processes are
cleaned up. Existing MongoDB volumes and services are never used.

Against a separately provisioned **test** replica set, these can also be run:

```bash
NAV_TEST_MONGO_URL='mongodb://127.0.0.1:27017/?replicaSet=navtest' \
  python -m pytest integration/ -q
MONGO_URL='mongodb://127.0.0.1:27017/?replicaSet=navtest' python scripts/check_nav.py
```

Default pytest `testpaths` excludes integration tests; explicit integration pytest
skips when `NAV_TEST_MONGO_URL` is absent. The fault helper creates a unique database
for each scenario and never drops a database selected by `MONGO_DATABASE`.
`check_backend.py` itself requires an isolated API/worker deployment: it creates
synthetic accounts there and removes only their records. Its NAV helper is copied
into the backend image with the synthetic fixture, so in an isolated Compose
deployment use `exec -T api python scripts/check_backend.py` (not stdin redirection).

Tests cover identical replay, malformed/zero-valid feeds, quality limits, oversized
stream early termination, byte/hash/date metadata, real unordered bulk failures
with rollback and prior-batch progress, concurrent claim attempts, overlapping
Airflow/worker entry points, lease renewal/expiry, stale transaction snapshots,
takeover ordered behind an open fenced transaction, worker-token replacement,
cancellation, closed-client disconnection, status reconciliation, metrics and
legacy unknown counts. No live AMFI endpoint is called.

Actual validation results are recorded in [nav-validation.md](nav-validation.md).

## Remaining limitations

- Whole-file atomicity is intentionally absent. Consumers can observe a mixture of
  old/new observations during a run, and failed attempts can have earlier batches
  committed. Read run counters/status and retry the retained source if needed.
- Fencing applies to upgraded ingestion service callers. Legacy writers or direct
  DB clients with collection write privileges can bypass it; MongoDB RBAC and a
  coordinated rollout are operational requirements, not implemented auth changes.
- The standalone configuration cannot safely run this fenced ingestion. Its
  refusal is deliberate. No existing topology conversion has been attempted.
- A crash/network outage may prevent saving the final error. Reconciliation runs
  when a worker is alive or a later ingestion claims the source. A stopped worker
  cannot reconcile itself. Source/job run status may lag until recovery.
- Driver retries can outlive a source attempt's initial operation; per-batch
  deadlines and fencing prevent a late old generation from overwriting a newer
  grant. A client-disconnect test is not a full multi-node failover/partition test.
- Metrics are multiple read queries, not an atomic dashboard snapshot. Counts may
  change during collection. Totals are operational history, not durable external
  counters if an operator deletes history. Sessions expire; monitoring clients
  need renewal or a future dedicated monitoring identity.
- Source dates use AMFI's day/month abbreviation parser and UTC quality thresholds;
  providers changing formats, holidays and heterogeneous fund dates need explicit
  policy decisions. Compatibility defaults do not enforce freshness thresholds.
- Local raw retention is optional and has no automatic disk lifecycle. Exact
  historical-source replay needs retained files. There is no S3/MinIO dependency.
- No live-provider, Airflow scheduler, browser interaction or multi-member failover
  test is implied by the local unit/integration results. MCP tools were not rewritten.
