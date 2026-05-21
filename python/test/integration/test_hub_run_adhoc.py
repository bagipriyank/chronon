"""Cloud-agnostic integration tests for ``zipline hub run-adhoc`` and ``zipline hub cancel``.

Replaces the former test_gcp_run_adhoc.py.
"""

import logging
import os

import pytest
from click.testing import CliRunner

from .helpers.cleanup import DataprocFlinkCleanup
from .helpers.cli import cancel_workflow, compile_configs, submit_run_adhoc
from .helpers.hub_api import get_flink_job_ids
from .helpers.workflow import poll_workflow_until

logger = logging.getLogger(__name__)

DEMO_BACKFILL = {
    "gcp": "compiled/joins/gcp/demo.v1__1",
    "aws": "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/joins/azure/demo.v2",
}


@pytest.fixture
def flink_cleanup(hub_url):
    """Cancel Dataproc Flink jobs associated with the workflow after the test."""
    ctx = {}
    yield ctx
    if "workflow_id" not in ctx:
        return
    workflow_id = ctx["workflow_id"]
    job_ids = get_flink_job_ids(hub_url, workflow_id)
    assert job_ids, (
        f"flink_cleanup: no Flink job IDs found for workflow {workflow_id}. "
        "A streaming Flink job may still be running on Dataproc — check and cancel manually."
    )
    project = os.environ.get("GCP_PROJECT_ID", "canary-443022")
    region = os.environ.get("GCP_REGION", "us-central1")
    DataprocFlinkCleanup(project, region).cancel_jobs(job_ids)


@pytest.mark.integration
def test_run_adhoc(confs, chronon_root, hub_url, cloud, flink_cleanup):
    """run-adhoc launches a streaming deploy and waits for SUCCEEDED."""
    if cloud != "gcp":
        pytest.skip(f"skipping test_run_adhoc for cloud: {cloud}")
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(
        runner, chronon_root, hub_url,
        confs(DEMO_BACKFILL[cloud]), "2025-08-01",
    )
    flink_cleanup["workflow_id"] = workflow_id

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"SUCCEEDED"}, timeout=1200, interval=15,
    )

@pytest.mark.integration
def test_run_adhoc_no_data(confs, chronon_root, hub_url, cloud):
    """run-adhoc with dates that have no input data should fail."""
    if cloud == "k8s":
        pytest.skip("k8s canary configs do not yet include a demo join for hub run-adhoc tests")
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(
        runner, chronon_root, hub_url,
        confs(DEMO_BACKFILL[cloud]), "1969-01-01",
    )

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"FAILED"}, timeout=1800, interval=30,
    )
