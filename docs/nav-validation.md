# NAV reliability validation and change inventory

Validated locally on 2026-11-11 using Python 3.11.15, MongoDB 7.0.23,
PyMongo 4.19.0, HTTPX 0.28.1, FastAPI 0.162.4 and pytest 9.2.0.
The repository dependency ranges were not changed. Test dependencies were installed
in `/tmp/bharatpay-nav-venv`, leaving the existing `.venv-api` untouched.

## Actual results

| Check | Result |
| --- | --- |
| Baseline `python -m pytest -q` in the existing environment | Could not start: existing `.venv-api` had no pytest or pip |
| Baseline in a restored isolated Python environment | 70 passed |
| First milestone: bounded sources and quality checks | 85 passed |
| Second milestone: fencing, lifecycle and faults | 92 offline tests passed; nine real-Mongo scenarios passed |
| Expanded monitoring regression | Initially **one failed, nine passed**: naive `hello.localTime` versus aware stored timestamps broke completed-run age calculation |
| UTC clock regression after fix | Targeted Mongo scenario passed; offline naive/aware clock regression added |
| Final `python -m pytest -q` | **94 passed in 1.02 seconds** |
| Final `python scripts/run_isolated_checks.py` | **Passed**, including all checks below |
| Backend API/worker checks | **27 HTTP checks passed**: auth, ownership, metrics authentication, active worker, CSV job/report, logout and expiry |
| NAV fault helper | **10 scenarios passed**; all sources synthetic or saved fixtures |
| Explicit `python -m pytest integration/ -q` | **10 passed in 11.19 seconds** |
| Actual CLI invocation with `--fixture` | Completed; CLI trigger, two newly inserted observations verified |
| Actual standalone Mongo server | Correctly refused NAV ingestion before HTTP/snapshot writes; failed run category `topology` |
| MCP and API import smoke check | Passed |
| Python AST parse and `node --check .../app.js` | Passed |
| Compose config with replica override and Airflow profile | Passed: new data mount, replica URI, init dependency and shared quality settings verified |
| Docker image build | **Could not start**: Docker daemon unavailable; no image build/runtime success claimed |
| Hosted GitHub Actions / Python 3.10 and 3.12 / Airflow scheduler | Not executed locally |

One validation command initially tried to inspect the optional Airflow service
without enabling its Compose profile and raised `KeyError: airflow`. Re-running
configuration validation with `--profile airflow` passed. Packaging review also
found that `.dockerignore` excluded fixtures; it now explicitly includes only
`tests/fixtures`, and the Dockerfile copies the helper scripts and fixtures.
The Docker build remains unverified because its daemon was unavailable.

The expected fault logs were sanitized `coordination_busy` and a run-ID-only
reconciliation warning after deliberately closing a test Mongo client. They are
not unhandled test failures. No test contacted AMFI. Temporary server/API/worker
processes created by the runner were stopped in its cleanup block. Existing
MongoDB services, credentials, accounts, volumes and application data were not used.

## Fault scenarios and their assertions

| Scenario in `scripts/check_nav.py` | Evidence |
| --- | --- |
| `replay_and_metadata` | Same source SHA/bytes; first run inserts two, replay matches two/inserts zero; date range, trigger, timestamps and unique index correct |
| `bad_feeds` | HTML, empty, NaN-only and quality failures save useful metadata and write zero snapshots |
| `bulk_failures` | Real Mongo validator fails an unordered bulk; current transaction rolls back; batch-size-one test preserves exactly one earlier committed batch and counter |
| `concurrent_entrypoints` | Paused Airflow-style ingestion excludes a worker; busy work defers without consuming an attempt, then completes with matched observations |
| `claims_and_stale_writers` | Twelve job claims yield one owner; eight NAV claims yield one owner; old transaction snapshot and old token cannot write after takeover |
| `expired_worker_token` | Reclaimed worker token blocks the old ingestion transaction even while it still has a global source token |
| `ordered_commit_and_reconciliation` | Acquisition cannot pass an uncommitted fenced transaction; after takeover old writes fail; live runs stay live, expired and abandoned-starting runs reconcile |
| `renewal_and_cancellation` | Two-second lease survives a three-second source through heartbeats; cancellation records interruption and releases ownership |
| `disconnect_and_reconcile` | Actually closed Mongo client cannot save final status; a recovered client and fenced takeover reconcile the stranded running attempt |
| `oversize_and_metrics` | Missing-length streamed response stops after two chunks and closes; metrics handle failure streaks, source freshness, exact write counts and unknown legacy counters |

These tests demonstrate fencing among upgraded cooperating service callers, not
RBAC enforcement against arbitrary collection writers. Client disconnection is
tested; a multi-node failover, network partition, unknown commit-result injection,
or live-provider contract test is not claimed. See the remaining limitations in
[the design](nav-reliability.md#remaining-limitations).

## Exact changes by file

| File | Change |
| --- | --- |
| `.dockerignore` | Keep synthetic fixtures in build context while excluding other tests |
| `.github/workflows/tests.yml` | Initialize an isolated replica set; run explicit Mongo fault tests; stop only the CI test container |
| `Dockerfile` | Include integration helper scripts and synthetic fixtures in the API/worker image |
| `README.md` | Fresh replica-set startup, source replay, test commands, metrics route and architecture diagram |
| `compose.yaml` | Configurable loopback API/Airflow ports and shared NAV quality environment settings |
| `compose.nav-replica.yaml` | Opt-in fresh named volume, replica initialization and application startup dependencies |
| `docs/nav-reliability-audit.md` | Baseline facts with code references, gaps and prioritized plan |
| `docs/nav-reliability.md` | Fencing design/proof, metadata semantics, quality controls, metrics, deployment/rollback and risks |
| `docs/nav-validation.md` | Actual successes/failures, environment, scenarios and this file inventory |
| `integration/test_nav_mongo.py` | Ten explicitly invoked pytest scenarios against generated temporary databases |
| `pipelines/nav_daily.py` | Airflow trigger metadata and permanent-error versus transient retry handling |
| `scripts/check_backend.py` | Authenticated metrics assertions and complete NAV fault helper invocation |
| `scripts/check_nav.py` | Synthetic source replay, real bulk/lease/concurrency/disconnection/reconciliation/metrics checks |
| `scripts/run_isolated_checks.py` | Disposable local Mongo/API/worker runner, actual CLI and standalone checks, scoped cleanup |
| `src/bharatpay_mcp/api/static/app.js` | Existing NAV page shows operational health and exact per-run write outcomes |
| `src/bharatpay_mcp/api/static/style.css` | Distinguish interrupted and starting/skipped states in status badges |
| `src/bharatpay_mcp/api/workbench.py` | Authenticated read-only `/ingestions/metrics` endpoint |
| `src/bharatpay_mcp/ingest.py` | Fixture CLI, explicit trigger, returned summary and sanitized DB errors |
| `src/bharatpay_mcp/services/ingestion.py` | Additive run lifecycle, bounded source handling, transactional fenced batches/counters/completion and sanitized failures |
| `src/bharatpay_mcp/services/nav.py` | Optional empty-result parsing for observable failures; deterministic duplicate-key rejection and safe rejection diagnostics |
| `src/bharatpay_mcp/services/nav_coordination.py` | Verified topology, atomic lease/version acquisition, transaction guard, renewal, release and fenced reconciliation |
| `src/bharatpay_mcp/services/nav_metrics.py` | Read-only ingestion/job/worker metrics and UTC clock normalization |
| `src/bharatpay_mcp/services/nav_source.py` | Stream limit, SHA-256, fixture source, optional raw retention and configurable quality rules |
| `src/bharatpay_mcp/storage.py` | Additive indexes, server-time atomic job claims, expiry finalization and reclaim counters |
| `src/bharatpay_mcp/worker.py` | Fenced NAV service invocation, categorized retry/defer behavior, presence and periodic reconciliation; sanitized logs |
| `tests/fixtures/nav_synthetic.txt` | Saved six/eight-column source with two valid and one invalid synthetic observation |
| `tests/test_nav_ingestion.py` | Offline topology, fence, metadata/error lifecycle, legacy metrics and UTC regression tests |
| `tests/test_nav_source.py` | Offline streaming, size, SHA, fixture, artifact, quality, duplicate and sanitized provider-error tests |

No MCP registration/tool module, dependency manifest or existing NAV unique index
was replaced. Unrelated pre-existing changes (the deleted desktop config example,
untracked `.sf/` and `AGENTS.md`) are intentionally outside these commits.

## Phase 2: integration, monitoring and deployment verification (2026-11-18)

Environment: macOS arm64, MongoDB 7.0.23 (`mongod` on PATH), fresh uv environments
for Python 3.10.20, 3.11 and 3.12.13 in temporary virtual environments. Docker 29.7.2,
Airflow 3.3.2 image, isolated Compose project `bharatpay-p2verify` with new volumes.
HTTP check counts vary between runs (62–68) because status polls are counted; the
assertions are identical.

| Check | Result | Label |
| --- | --- | --- |
| `pytest tests/ -q` on 3.10 / 3.11 / 3.12 | **121 passed** on each (94 previous + 27 new) | Runtime-tested |
| `scripts/run_isolated_checks.py` | **Passed**: 68 HTTP checks, 11 NAV scenarios, 11 integration pytest, CLI replay, standalone refusal, alert exit code | Runtime-tested |
| Two workers on one queue | 6 concurrent CSV jobs × 20 rows: each completed with `attempts == 1`, exactly 20 row documents per job | Runtime-tested |
| API → job → worker → fenced NAV ETL | 202, duplicate queue 409, worker run `trigger=worker`, `source_kind=fixture`, history readable | Runtime-tested |
| Retry after partial failure | Retried job re-completes with the same 2 row documents | Runtime-tested |
| Health classification (`health_signals`) | never-executed, per-trigger failures, stalled vs polling worker, queue wait, reclaim streaks on real Mongo | Runtime-tested |
| `check_health.py` exit codes | Exits 1 with `FAILED worker` when no worker presence exists | Runtime-tested |
| Compose config: 4 supported combinations + fixture override | All valid; new volume, loopback ports, no Mongo host port | Runtime-tested (client-side) |
| Docker image build (API, worker, Airflow 3.3.2) | Built; pip hash verification on. One earlier attempt failed a pymongo wheel hash check through Docker Desktop's proxy right after a forced daemon restart; host downloads matched PyPI and the retry passed | Runtime-tested |
| Stack startup (`bharatpay-p2verify`, fresh volumes) | mongo, api, worker healthy in 25 s; `mongo-init` exit 0, and exit 0 again when re-run on the initialized set | Runtime-tested |
| Replica set | `bharatpay-rs`, one PRIMARY, writable | Runtime-tested |
| `/health`, `/ready`, metrics auth | 200, 200 (`persistence: true`), 401 without token | Runtime-tested |
| Backend + NAV fault suite inside the API container | 62 HTTP checks and 11 NAV scenarios passed on the final images | Runtime-tested |
| Mongo stopped for 20 s | Worker kept running (0 restarts), logged sanitized warnings, recovered healthy; `/ready` 503; `check_health.py` exit 2 | Runtime-tested |
| Worker process SIGKILL | Container restarted by policy within 3 s; healthy again | Runtime-tested |
| Health script on the live stack | All six components healthy, exit 0, with `HEALTH_EXPECT_AIRFLOW=1` | Runtime-tested |
| Airflow DAG import and contract (`check_dag.py`) | Passed in the Airflow 3.3.2 image | Runtime-tested |
| Airflow task execution (`airflow dags test`) | See scenario table below | Runtime-tested |
| CI `containers` job | Written; not executed on GitHub | Not yet verified |
| Static DAG contract (AST) | id, schedule, catchup, max runs, single task, error mapping present | Runtime-tested (offline) |

### Airflow scenarios (real `airflow dags test`, isolated stack, synthetic sources)

| Scenario | Fault injected | Airflow outcome | MongoDB `ingestion_runs` |
| --- | --- | --- | --- |
| Success | none (fixture) | task `success`, run successful | `completed`, `trigger=airflow`, accepted 2, rejected 1 |
| Invalid records | fixture has one invalid row | `success` | `rejected_count=1` recorded, valid rows written |
| Duplicate ingestion | same fixture again | `success` | `completed`, 0 inserted, 2 matched |
| Zero valid records | mounted file with only invalid rows | `AirflowFailException`, `failed`, **no retry** | `failed`, `parse`, 0 written |
| Provider failure | HTTPS proxy to a closed port (AMFI never contacted) | 3 attempts 5 min apart (14:10:48, 14:15:50, 14:20:52), then `failed` | 3 × `failed`, `source_network` |
| Database failure | unresolvable `MONGO_URL` | 3 attempts 5 min apart, then `failed` | none: the DB is the failure; only Airflow logs record it |
| Ingestion already running | live foreign NAV lease seeded | 2 × `up_for_retry` (`IngestionBusy`) | 2 × `skipped`, `coordination_busy` |
| Retryable failure then success | same run: the foreign lease expired before attempt 3 | 3rd attempt `success` | `completed` |
| Recovery after interruption | abandoned `running` run with an expired lease | `success` | abandoned run → `interrupted`, `write_outcome=partial_or_unknown`; lease reclaims 2 |

Running against the actual Airflow image found two problems that offline checks
missed. The DAG imported `AirflowFailException` from `airflow.exceptions`, which
is deprecated in 3.3 (same class, so behavior was already correct); it now uses
`airflow.sdk.exceptions`. `check_dag.py` assumed `DagBag` and timetable APIs
that 3.3 does not have. Both are fixed and re-verified.

Review findings fixed before commit: fixture-sourced runs and snapshots recorded
`source` as the AMFI URL, so synthetic NAV rows looked like live data. They now
record `source=file:<name>` and `source_kind=fixture` (verified on the rebuilt
stack). `renewal_and_cancellation` failed once in 11 runs at load average ~11
with a 1-second lease: a renewal arrived late and the fence correctly refused the
write. Its margin was widened (2 s lease, 3 s source); later runs passed.

Found while inspecting (read-only) the pre-existing `bharatpay-mcp` Compose project:
after Docker Desktop restarted, only its worker (`restart: unless-stopped`) came back.
`mongo` and `api` had no restart policy, so the worker crash-looped (restart count 7)
with a full `ServerSelectionTimeoutError` traceback. Fixed in configuration
(restart policies) and code (startup backoff); that project itself was not touched.

Not demonstrated by any test: worker SIGKILL **mid-job** followed by reclaim after
a real 120 s lease expiry (an idle worker kill/restart and the expired-lease claim
path are tested separately), Airflow under a live scheduler (`dags test` runs the
task in-process with real retry delays), multi-member
failover, network partitions, and live AMFI contract. Guarantees shown: one lease
holder per claim among 12 concurrent claimants; no stale NAV batch commits after a
newer lease grant; stable row ids make CSV retries idempotent. Execution remains at
least once, not exactly once.
