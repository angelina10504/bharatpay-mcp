"""Verify the NAV DAG inside the Airflow image; no database or network is used.

Run in the container: python /opt/project/scripts/check_dag.py
Checks that the DAG imports with the installed Airflow and project packages,
that its scheduling contract is unchanged, and that the task maps permanent
ingestion errors to AirflowFailException while letting transient ones retry.
"""
from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
from unittest.mock import patch

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # older Airflow 3 releases
    from airflow.models.dagbag import DagBag
from airflow.sdk.exceptions import AirflowFailException

from bharatpay_mcp.services.nav_source import IngestionError

FOLDER = Path(os.getenv("AIRFLOW__CORE__DAGS_FOLDER", Path(__file__).resolve().parents[1] / "pipelines"))


def main():
    bag = DagBag(dag_folder=str(FOLDER))
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["bharatpay_daily_nav"]
    assert [task.task_id for task in dag.tasks] == ["ingest_daily_nav"]
    task = dag.get_task("ingest_daily_nav")
    assert (task.retries, task.retry_delay) == (2, timedelta(minutes=5))
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert dag.timetable.expression == "0 19 * * 1-5" and str(dag.timetable.timezone) == "UTC"
    print("PASS DAG: imports cleanly; one task, weekdays 19:00 UTC, two 5-minute retries, no catchup")

    for category, expected in (("quality", AirflowFailException), ("parse", AirflowFailException),
                               ("topology", AirflowFailException), ("source_timeout", IngestionError),
                               ("database", IngestionError), ("coordination_busy", IngestionError)):
        async def failing(**kwargs):
            assert kwargs == {"trigger": "airflow"}
            raise IngestionError(category, "synthetic " + category)
        with patch("bharatpay_mcp.ingest.main", failing):
            try:
                task.python_callable()
            except expected as error:
                # A retryable error must not be converted into a permanent failure.
                assert type(error) is expected, (category, type(error))
            else:
                raise AssertionError("Task swallowed " + category)
    print("PASS DAG: permanent errors fail without retry; transient errors propagate for Airflow retries")


if __name__ == "__main__":
    main()
