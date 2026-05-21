from ai.chronon.types import EngineType, StagingQuery, TableDependency


def get_native_partition_export(table: str, partition_column: str = "ds", version: int = 0):
    spark_sql = f"""
    SELECT
        *
    FROM {table}
    WHERE
    {partition_column} BETWEEN '{{{{ start_date }}}}' AND '{{{{ end_date }}}}'
    """
    return StagingQuery(
        query=spark_sql,
        output_namespace="data",
        engine_type=EngineType.SPARK,
        dependencies=[TableDependency(table=table, partition_column=partition_column, offset=0)],
        version=version,
        step_days=30,
    )


user_activities = get_native_partition_export("user_activities")
checkouts = get_native_partition_export("checkouts")
