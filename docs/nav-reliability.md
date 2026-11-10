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
