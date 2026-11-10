"""Optional local Airflow DAG; shares the standalone idempotent ETL."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task

with DAG(
    dag_id="bharatpay_daily_nav",
    schedule="0 19 * * 1-5",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bharatpay", "etl"],
) as dag:
    @task
    def ingest_daily_nav():
        from bharatpay_mcp.ingest import main
        from airflow.exceptions import AirflowFailException
        from bharatpay_mcp.services.nav_source import IngestionError
        try:
            return asyncio.run(main(trigger="airflow"))
        except IngestionError as error:
            if not error.retryable:
                raise AirflowFailException(str(error)) from None
            raise

    ingest_daily_nav()
