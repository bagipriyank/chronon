from group_bys.aws_databricks import dim_listings
from staging_queries.aws_databricks import exports

from ai.chronon.types import EntitySource, Join, JoinPart, Query, selects

v1 = Join(
    left=dim_listings.source,
    row_ids=[],
    right_parts=[
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
    enable_stats_compute=True,
)

# Four partitioned/unpartitioned × dense/sparse variants exercised by
# test_hub_backfill_uc.py against the Crucible-backed AWS Hub.
_left_selects = selects(listing_id="listing_id")

pt_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_pt.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.pt_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)

sparse_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_sparse.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.sparse_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)

unpartitioned_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_unpartitioned.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.unpartitioned_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)

unpartitioned_sparse_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_unpartitioned_sparse.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.unpartitioned_sparse_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)
