"""Hub backfill against Unity Catalog / Delta canary confs (AWS only).

Exercises the Crucible-backed AWS Hub end-to-end against the `aws_databricks`
team's UC + Delta join variants, instead of the Glue-Hive `aws/...` path used by
`test_hub_backfill.py`.

Four variants cover the partitioning × density matrix:
  - demo.pt_v1                  — partitioned dense
  - demo.sparse_v1              — partitioned sparse
  - demo.unpartitioned_v1       — unpartitioned dense
  - demo.unpartitioned_sparse_v1 — unpartitioned sparse
"""

import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

# aws_databricks ships canary EMR Serverless wiring in teams.py (PROD compile,
# `compiled/`) and crucible-aws K8sSubmitter overrides in teams.canary.py (canary
# compile, `compiled_canary/`). The deploying CI picks which set is read by
# setting ZIPLINE_COMPILE_ENV. Default "prod" keeps update_canary.yaml untouched.
_COMPILE_ENV = os.environ.get("ZIPLINE_COMPILE_ENV", "prod").strip() or "prod"
_COMPILED_DIR = "compiled" if _COMPILE_ENV == "prod" else f"compiled_{_COMPILE_ENV}"

UC_DEMO_VARIANTS = [
    f"{_COMPILED_DIR}/joins/aws_databricks/demo.pt_v1",
    f"{_COMPILED_DIR}/joins/aws_databricks/demo.sparse_v1",
    f"{_COMPILED_DIR}/joins/aws_databricks/demo.unpartitioned_v1",
    f"{_COMPILED_DIR}/joins/aws_databricks/demo.unpartitioned_sparse_v1",
]


@pytest.mark.integration
@pytest.mark.parametrize("conf_path", UC_DEMO_VARIANTS, ids=lambda p: p.rsplit(".", 1)[-1])
def test_backfill_uc_demo(confs, chronon_root, hub_url, cloud, conf_path):
    """Multi-day backfill of an aws_databricks/demo.* variant via UC + Delta.

    Uses the ``confs`` fixture so each run hits a fresh test_id-scoped conf
    name. Without this, Hub fast-skips on the second run because the output
    Iceberg partitions for the previous run still exist (workflow reports
    SUCCEEDED with zero work actually executed).
    """
    if cloud != "aws":
        pytest.skip(f"UC backfill is AWS-only; cloud={cloud}")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    # 2026-02-01 → 2026-02-05 is the consistent window across all 4 variants:
    # the dense source tables (dim_listings_pt / dim_listings_unpartitioned)
    # are populated for every date, while the sparse sources
    # (dim_listings_sparse / dim_listings_unpartitioned_sparse) have data at
    # 02-01, 02-03, 02-05 and gaps at 02-02, 02-04. Endpoints exist so
    # chronon's range check passes; the intra-range gaps exercise the
    # sparse-data code paths end-to-end.
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs(conf_path), "2026-02-01", "2026-02-05",
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)
