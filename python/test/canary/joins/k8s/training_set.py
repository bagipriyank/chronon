from group_bys.k8s import purchases

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

source = EventSource(
    table="data.checkouts",
    query=Query(
        selects=selects("user_id"),
        time_column="ts",
    ),
)

v1_test = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.v1_test),
    ],
    version=0,
)

v1_dev = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.v1_dev),
    ],
    version=0,
)
