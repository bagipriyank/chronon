"""Cloud-agnostic integration test for the full zipline run pipeline.

Exercises: compile -> import staging queries -> backfill group_bys -> backfill joins
-> check partitions -> upload -> upload-to-kv -> metadata-upload
-> staging query -> fetch

Replaces the former test_gcp_template_quickstart.py.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from click.testing import CliRunner

from .helpers.cli import (
    compile_configs,
    submit_check_partitions,
    submit_fetch,
    submit_metadata_upload,
    submit_run,
    submit_run_subprocess,
    submit_upload,
    submit_upload_to_kv,
)

START_DS = {
    "gcp": "2023-11-01",
    "aws": "2026-01-07",
    "azure": "2023-11-01",
    "k8s": "2023-11-01",
}

END_DS = {
    "gcp": "2023-11-30",
    "aws": "2026-01-18",
    "azure": "2023-11-30",
    "k8s": "2023-11-30",
}

STAGING_QUERY_IMPORT_KEYS = {
    "gcp": [
        "compiled/staging_queries/gcp/purchases_import.v1__0",
        "compiled/staging_queries/gcp/checkouts_import.v1__0",
        "compiled/staging_queries/gcp/purchases_notds_import.v1__0",
        "compiled/staging_queries/gcp/checkouts_notds_import.v1__0",
    ],
    "aws": [
        "compiled/staging_queries/aws/exports.user_activities__0",
        "compiled/staging_queries/aws/exports.dim_listings__0",
        "compiled/staging_queries/aws/exports.dim_merchants__0",
    ],
    "azure": [
        "compiled/staging_queries/azure/exports.user_activities__0",
        "compiled/staging_queries/azure/exports.checkouts__0",
    ],
    "k8s": [
        "compiled/staging_queries/k8s/exports.user_activities__0",
        "compiled/staging_queries/k8s/exports.checkouts__0",
    ],
}

GROUP_BY_KEY = {
    "gcp": "compiled/group_bys/gcp/purchases.v1_test__0",
    "aws": "compiled/group_bys/aws/user_activities.v1__1",
    "azure": "compiled/group_bys/azure/purchases.v1_test__0",
    "k8s": "compiled/group_bys/k8s/purchases.v1_test__0",
}

JOIN_KEY = {
    "gcp": "compiled/joins/gcp/training_set.v1_test__0",
    "aws": "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/joins/azure/training_set.v1_test__0",
    "k8s": "compiled/joins/k8s/training_set.v1_test__0",
}


@pytest.mark.integration
def test_run_quickstart(test_id, confs, chronon_root, version, cloud):
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    start_ds = START_DS[cloud]
    end_ds = END_DS[cloud]

    # 1. Import staging queries / run exports (parallel — jobs are independent)
    staging_keys = STAGING_QUERY_IMPORT_KEYS[cloud]
    with ThreadPoolExecutor(max_workers=len(staging_keys) or 1) as pool:
        futures = {
            pool.submit(
                submit_run_subprocess,
                chronon_root, confs(key), version,
                start_ds=start_ds, end_ds=end_ds,
            ): key
            for key in staging_keys
        }
        for future in as_completed(futures):
            future.result()

    # 2. Backfill group_by
    gb_conf = confs(GROUP_BY_KEY[cloud])
    submit_run(runner, chronon_root, gb_conf, version,
               start_ds=start_ds, end_ds=end_ds)

    # 3. Backfill joins (parallel — both depend on group_by, not on each other)
    join_conf = confs(JOIN_KEY[cloud])
    notds_key = f"compiled/joins/{cloud}/training_set.v1_dev_notds__0"
    join_confs = [join_conf]
    notds_conf = confs(notds_key)
    if os.path.exists(os.path.join(chronon_root, notds_conf)):
        join_confs.append(notds_conf)
    if len(join_confs) > 1:
        with ThreadPoolExecutor(max_workers=len(join_confs)) as pool:
            futures = [
                pool.submit(
                    submit_run_subprocess,
                    chronon_root, conf, version,
                    start_ds=start_ds, end_ds=end_ds,
                )
                for conf in join_confs
            ]
            for future in as_completed(futures):
                future.result()
    else:
        submit_run(runner, chronon_root, join_confs[0], version,
                   start_ds=start_ds, end_ds=end_ds)

    # Steps 4-9 are only supported by the GCP runner (Dataproc).
    # The AWS runner (EMR) does not implement metastore, upload, or fetch modes.
    if cloud == "gcp":
        # 4. Check partitions
        partition_name = f"data.{cloud}_purchases_{test_id}_v1_test__0/ds={end_ds}"
        submit_check_partitions(
            runner, chronon_root,
            f"compiled/teams_metadata/{cloud}/{cloud}_team_metadata",
            version, partition_name,
        )

        # 5. Upload
        submit_upload(runner, chronon_root, gb_conf, version, ds=end_ds)

        # 6. Upload to KV
        submit_upload_to_kv(runner, chronon_root, gb_conf, version, ds=end_ds)

        # 7. Metadata upload
        submit_metadata_upload(runner, chronon_root, gb_conf, version)

        # 8. Staging query (exports)
        exports_conf = confs(f"compiled/staging_queries/{cloud}/exports.checkouts__0")
        submit_run(runner, chronon_root, exports_conf, version,
                   start_ds=start_ds, end_ds=end_ds)

        # 9. Fetch and verify
        fetch_name = f"{cloud}.purchases_{test_id}.v1_test__0"
        result = submit_fetch(runner, chronon_root, gb_conf, version,
                              keys='{"user_id":"5"}', name=fetch_name)
        assert "purchase_price_average_7d" in result.output, \
            f"Expected purchase_price_average_7d in fetch output:\n{result.output}"
