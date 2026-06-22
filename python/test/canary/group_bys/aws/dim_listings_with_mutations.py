from ai.chronon.group_by import DefaultAggregation
from ai.chronon.types import Accuracy, EntitySource, GroupBy, Query, selects

"""
Mirrors dim_listings.v1 but uses an EntitySource with a mutation_table and
mutation_topic to support Chronon's EntitySource with Accuracy.TEMPORAL.

This enables point-in-time accurate lookups against listing dimensions —
feature values reflect the exact state of a listing at any historical
timestamp, not just the daily snapshot.

Pipeline:
  - snapshot_table: demo.dim_listings_snapshot — daily end-of-day listing state
  - mutation_table: demo.dim_listings_mutations — full Debezium CDC history,
    partitioned by ds, with mutation_ts (ms) and is_before columns
  - mutation_topic: kinesis://dim-listings-mutations — real-time CDC stream
    in Debezium JSON format (schemas.enable=false), published at ~1 event/15s
"""

source = EntitySource(
    snapshot_table="demo.dim_listings_snapshot",
    mutation_table="demo.dim_listings_mutations",
    mutation_topic="kinesis://dim-listings-mutations/serde=glue_registry/registry_name=zipline-canary/schema_name=dim-listings-mutations",
    query=Query(
        selects=selects(
            listing_id="listing_id",
            merchant_id="merchant_id",
            headline="headline",
            brief_description="brief_description",
            long_description="long_description",
            price_cents="price_cents",
            currency="currency",
            inventory_count="inventory_count",
            primary_category="primary_category",
            is_active="is_active",
            weight_grams="weight_grams",
            tags="tags",
            # Derived features
            is_expensive="IF(price_cents > 10000, 1, 0)",  # Over $100
            is_in_stock="IF(inventory_count > 0, 1, 0)",
            main_image_path="main_image_path",
            secondary_image_paths="secondary_image_paths",
        ),
        start_partition="2025-01-01",
        time_column="ts"
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["listing_id"],
    online=False,
    version=3,
    accuracy=Accuracy.TEMPORAL,
    aggregations=DefaultAggregation(keys=["listing_id"], sources=[source]),
    step_days=10,
)
