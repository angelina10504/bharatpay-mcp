"""Explicit integration suite; excluded from the default offline testpaths."""
from __future__ import annotations

import asyncio
import os

import pytest

from scripts import check_nav


@pytest.mark.parametrize("scenario", [
    check_nav.replay_and_metadata, check_nav.bad_feeds, check_nav.bulk_failures,
    check_nav.concurrent_entrypoints, check_nav.claims_and_stale_writers,
    check_nav.expired_worker_token, check_nav.renewal_and_cancellation,
    check_nav.disconnect_and_reconcile, check_nav.ordered_commit_and_reconciliation,
    check_nav.oversize_and_metrics,
])
def test_isolated_nav_scenario(scenario):
    url = os.environ.get("NAV_TEST_MONGO_URL")
    if not url:
        pytest.skip("Set NAV_TEST_MONGO_URL to an isolated test replica set")
    asyncio.run(asyncio.wait_for(scenario(url), 45))
