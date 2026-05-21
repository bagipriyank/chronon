"""Cloud-agnostic integration tests for the schedule lifecycle: deploy, verify, delete.

Replaces the former test_gcp_schedule.py.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_schedule
from .helpers.hub_api import delete_schedule, find_schedules_by_test_id

DEMO_BACKFILL = {
    "gcp": "compiled/joins/gcp/demo.v1__1",
    "aws": "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/joins/azure/demo.v2",
}


@pytest.mark.integration
def test_schedule_lifecycle(confs, test_id, chronon_root, hub_url, cloud):
    """Deploy a schedule, verify it exists, delete it, verify it's gone."""
    if cloud == "k8s":
        pytest.skip("k8s canary configs do not yet include a demo join for hub schedule tests")
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    submit_schedule(runner, chronon_root, hub_url, confs(DEMO_BACKFILL[cloud]))

    schedules = find_schedules_by_test_id(hub_url, test_id)
    assert len(schedules) == 2, f"No schedules found for test_id={test_id}"
    conf_name = schedules[0]["confName"]

    delete_schedule(hub_url, conf_name)

    schedules = find_schedules_by_test_id(hub_url, test_id)
    assert len(schedules) == 0, f"Schedules still present after delete: {schedules}"
