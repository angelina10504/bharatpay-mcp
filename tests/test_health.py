"""Offline health classification, source override and DAG contract; no DB or network."""
from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from bharatpay_mcp.services.nav_metrics import HealthLimits, assess, worker_state
from bharatpay_mcp.services.nav_source import configured_source_file

STAMP = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]


def run(status="completed", category=None):
    return {"status": status, "error_category": category}


def trigger(latest=None, completed=None, age=None, failures=0):
    return {"latest_attempt": latest, "last_completed": completed,
            "last_completed_age_seconds": age, "failures_since_success": failures}


def metrics(**overrides):
    base = {
        "transaction_capable": True,
        "latest_attempt": run(), "last_completed": run(), "last_completed_age_seconds": 3600,
        "consecutive_failures": 0,
        "by_trigger": {"airflow": trigger()},
        "queue": {"oldest_queued_age_seconds": None, "max_active_lease_reclaims": 0, "lease_expired_failures_24h": 0},
        "job_lease_expired": 0,
        "workers": {"active": 1, "polling": 1, "busy": 0, "stalled": 0, "last_heartbeat_at": STAMP},
        "lease": {"expired": False},
    }
    base.update(overrides)
    return base


def states(result):
    return {name: part["status"] for name, part in result["components"].items()}


def test_all_healthy_with_airflow_not_configured():
    result = assess(metrics(), HealthLimits())
    assert states(result) == {"database": "healthy", "worker": "healthy", "nav_ingestion": "healthy",
                              "airflow_schedule": "not_configured", "job_queue": "healthy", "leases": "healthy"}
    assert result["status"] == "not_configured"


def test_expected_airflow_without_runs_is_never_executed():
    result = assess(metrics(), HealthLimits(expect_airflow=True))
    assert states(result)["airflow_schedule"] == "never_executed"


def test_nothing_ingested_yet_is_never_executed_not_failed():
    result = assess(metrics(latest_attempt=None, last_completed=None, last_completed_age_seconds=None),
                    HealthLimits())
    assert states(result)["nav_ingestion"] == "never_executed"


@pytest.mark.parametrize("failures,expected", [(1, "degraded"), (2, "degraded"), (3, "failed")])
def test_failure_streak_escalates(failures, expected):
    result = assess(metrics(latest_attempt=run("failed", "source_timeout"), consecutive_failures=failures),
                    HealthLimits())
    assert states(result)["nav_ingestion"] == expected
    assert "source_timeout" in result["components"]["nav_ingestion"]["detail"]


def test_failed_scheduled_airflow_run_is_reported_separately():
    airflow = trigger(run("failed", "database"), run(), 7200, failures=3)
    result = assess(metrics(by_trigger={"airflow": airflow}), HealthLimits(expect_airflow=True))
    assert states(result)["airflow_schedule"] == "failed"
    assert result["status"] == "failed"


def test_only_failures_and_no_success_is_failed():
    result = assess(metrics(latest_attempt=run("failed", "topology"), last_completed=None,
                            last_completed_age_seconds=None, consecutive_failures=1), HealthLimits())
    assert states(result)["nav_ingestion"] == "failed"


def test_old_success_is_degraded():
    result = assess(metrics(last_completed_age_seconds=97 * 3600), HealthLimits())
    assert states(result)["nav_ingestion"] == "degraded"


def test_standalone_database_is_degraded():
    assert states(assess(metrics(transaction_capable=False), HealthLimits()))["database"] == "degraded"


@pytest.mark.parametrize("workers,expected", [
    ({"active": 0, "polling": 0, "busy": 0, "last_heartbeat_at": None}, "failed"),
    ({"active": 0, "polling": 0, "busy": 0, "last_heartbeat_at": STAMP - timedelta(minutes=5)}, "failed"),
    ({"active": 1, "polling": 0, "busy": 0, "last_heartbeat_at": STAMP}, "degraded"),
    ({"active": 1, "polling": 0, "busy": 1, "last_heartbeat_at": STAMP}, "healthy"),
])
def test_worker_needs_loop_progress_not_just_presence(workers, expected):
    assert states(assess(metrics(workers=workers), HealthLimits()))["worker"] == expected


@pytest.mark.parametrize("active,expected", [(1, "degraded"), (0, "failed")])
def test_long_queue_wait(active, expected):
    workers = {"active": active, "polling": active, "busy": 0, "last_heartbeat_at": STAMP}
    result = assess(metrics(workers=workers, queue={"oldest_queued_age_seconds": 301,
                                                    "max_active_lease_reclaims": 0, "lease_expired_failures_24h": 0}),
                    HealthLimits())
    assert states(result)["job_queue"] == expected


@pytest.mark.parametrize("overrides", [
    {"job_lease_expired": 1},
    {"lease": {"expired": True}},
    {"queue": {"oldest_queued_age_seconds": None, "max_active_lease_reclaims": 2, "lease_expired_failures_24h": 0}},
    {"queue": {"oldest_queued_age_seconds": None, "max_active_lease_reclaims": 0, "lease_expired_failures_24h": 1}},
])
def test_expired_or_repeatedly_reclaimed_leases_degrade(overrides):
    assert states(assess(metrics(**overrides), HealthLimits()))["leases"] == "degraded"


def test_worker_state_classification():
    fresh = {"heartbeat_at": STAMP}
    assert worker_state({**fresh, "polled_at": STAMP}, STAMP) == "polling"
    assert worker_state({**fresh, "polled_at": STAMP, "current_job_id": "job"}, STAMP) == "busy"
    assert worker_state({**fresh, "polled_at": STAMP - timedelta(minutes=2)}, STAMP) == "stalled"
    assert worker_state(fresh, STAMP) == "stalled"  # pre-upgrade worker: presence only
    assert worker_state({"heartbeat_at": STAMP - timedelta(minutes=1), "polled_at": STAMP}, STAMP) == "stale"


@pytest.mark.parametrize("values", [(0, 96, 300), (3, float("nan"), 300), (3, 96, -1)])
def test_invalid_limits_rejected(values):
    with pytest.raises(ValueError):
        HealthLimits(*values)


def test_limits_from_env(monkeypatch):
    monkeypatch.setenv("HEALTH_MAX_CONSECUTIVE_FAILURES", "5")
    monkeypatch.setenv("HEALTH_EXPECT_AIRFLOW", "true")
    limits = HealthLimits.from_env()
    assert limits.max_consecutive_failures == 5 and limits.expect_airflow


def test_source_file_override(monkeypatch):
    monkeypatch.delenv("NAV_SOURCE_FILE", raising=False)
    assert configured_source_file() is None
    monkeypatch.setenv("NAV_SOURCE_FILE", "/data/nav.txt")
    assert configured_source_file() == Path("/data/nav.txt")


def test_dag_contract_without_airflow():
    """Static guard; scripts/check_dag.py imports the DAG with real Airflow."""
    tree = ast.parse((ROOT / "pipelines/nav_daily.py").read_text())
    dag = next(node for node in ast.walk(tree) if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "DAG")
    kwargs = {k.arg: k.value for k in dag.keywords}
    assert ast.literal_eval(kwargs["dag_id"]) == "bharatpay_daily_nav"
    assert ast.literal_eval(kwargs["schedule"]) == "0 19 * * 1-5"
    assert ast.literal_eval(kwargs["catchup"]) is False and ast.literal_eval(kwargs["max_active_runs"]) == 1
    tasks = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
             and any(getattr(d, "id", "") == "task" for d in node.decorator_list)]
    assert tasks == ["ingest_daily_nav"]
    source = ast.unparse(tree)
    assert "main(trigger='airflow')" in source and "AirflowFailException" in source
