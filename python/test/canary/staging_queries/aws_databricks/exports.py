from ai.chronon.types import EngineType, StagingQuery, TableDependency

dim_listings = StagingQuery(
    query="""
    SELECT
        *
    FROM workspace.poc.dim_listings
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings", partition_column="ds", offset=0)
    ],
    version=0,
)

dim_listings_non_partitioned = StagingQuery(
    query="""
    SELECT
        *, DATE_FORMAT(updated_at_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_nop
    WHERE
    DATE_FORMAT(updated_at_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_nop", partition_column="updated_at_ts", offset=0)
    ],
    version=0,
)

# Partition testing
dim_listings_pt = StagingQuery(
    query="""
    SELECT * FROM workspace.poc.dim_listings
    WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings", partition_column="ds", offset=0)
    ],
    version=1,
    step_days=30,
)

dim_listings_sparse = StagingQuery(
    query="""
    SELECT * FROM workspace.poc.dim_listings_sparse
    WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_sparse", partition_column="ds", offset=0)
    ],
    version=0,
    step_days=30,
)

dim_listings_unpartitioned = StagingQuery(
    query="""
    SELECT *,
        DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_unpartitioned
    WHERE DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_unpartitioned", partition_column="snapshot_ts", offset=0)
    ],
    version=0,
    step_days=30,
)

dim_listings_unpartitioned_sparse = StagingQuery(
    query="""
    SELECT *,
        DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_unpartitioned_sparse
    WHERE DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_unpartitioned_sparse", partition_column="snapshot_ts", offset=0)
    ],
    version=0,
    step_days=30,
)

# Bug repro: recompute_days causes duplicates on Iceberg
dim_listings_recompute = StagingQuery(
    query="""
    SELECT *,
        DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_unpartitioned
    WHERE DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_unpartitioned", partition_column="snapshot_ts", offset=0)
    ],
    version=0,
    recompute_days=3,
    step_days=30,
)
