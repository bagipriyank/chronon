from ai.chronon.types import (
    Aggregation,
    EventSource,
    GroupBy,
    Operation,
    Query,
    TimeUnit,
    Window,
    selects,
)

source = EventSource(
    table="data.purchases",
    query=Query(
        selects=selects("user_id", "purchase_price"),
        start_partition="2023-11-01",
        time_column="ts",
    ),
)

window_sizes = [Window(length=day, time_unit=TimeUnit.DAYS) for day in [1, 3, 7]]

v1_dev = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    version=0,
    aggregations=[
        Aggregation(input_column="purchase_price", operation=Operation.SUM, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.COUNT, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.AVERAGE, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.LAST_K(10)),
    ],
)

v1_test = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    version=0,
    aggregations=[
        Aggregation(input_column="purchase_price", operation=Operation.SUM, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.COUNT, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.AVERAGE, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.LAST_K(10)),
    ],
)
