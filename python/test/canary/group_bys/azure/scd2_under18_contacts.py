"""Single-file SCD2 chain for counting minor contacts on a claim.

This intentionally keeps staging queries, sources, groupBys, and joins together
so the local expectation runner can exercise the full dependency chain from one
config file.
"""

from ai.chronon import scd2
from ai.chronon.types import (
    Derivation,
    EngineType,
    EventSource,
    Join,
    JoinPart,
    JoinSource,
    Query,
    StagingQuery,
    TableDependency,
    selects,
)

STEP_DAYS = 2
VERSION = 1


claim_export_sq = StagingQuery(
    query="""
        SELECT
            id,
            claim_number,
            CAST(unix_timestamp(record_begin_timestamp) * 1000 AS BIGINT) AS record_begin_timestamp,
            CAST(unix_timestamp(record_end_timestamp) * 1000 AS BIGINT) AS record_end_timestamp,
            CAST(record_begin_timestamp AS DATE) AS ds
        FROM default.atlas_cc_claim
        WHERE CAST(record_begin_timestamp AS DATE) BETWEEN {{ start_date }} AND {{ end_date }}
          AND retired = 0
        """,
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(
            table="default.atlas_cc_claim",
            partition_column="ds",
            start_offset=0,
            end_offset=0,
        ),
    ],
    version=VERSION,
    step_days=STEP_DAYS,
)

claimcontact_export_sq = StagingQuery(
    query="""
        SELECT
            id,
            contactid,
            claimid,
            CAST(unix_timestamp(record_begin_timestamp) * 1000 AS BIGINT) AS record_begin_timestamp,
            CAST(unix_timestamp(record_end_timestamp) * 1000 AS BIGINT) AS record_end_timestamp,
            CAST(record_begin_timestamp AS DATE) AS ds
        FROM default.atlas_cc_claimcontact
        WHERE CAST(record_begin_timestamp AS DATE) BETWEEN {{ start_date }} AND {{ end_date }}
          AND retired = 0
        """,
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(
            table="default.atlas_cc_claimcontact",
            partition_column="ds",
            start_offset=0,
            end_offset=0,
        ),
    ],
    version=VERSION,
    step_days=STEP_DAYS,
)

contact_export_sq = StagingQuery(
    query="""
        SELECT
            id,
            dateofbirth,
            CAST(unix_timestamp(record_begin_timestamp) * 1000 AS BIGINT) AS record_begin_timestamp,
            CAST(unix_timestamp(record_end_timestamp) * 1000 AS BIGINT) AS record_end_timestamp,
            CAST(record_begin_timestamp AS DATE) AS ds
        FROM default.atlas_cc_contact
        WHERE CAST(record_begin_timestamp AS DATE) BETWEEN {{ start_date }} AND {{ end_date }}
          AND retired = 0
        """,
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(
            table="default.atlas_cc_contact",
            partition_column="ds",
            start_offset=0,
            end_offset=0,
        ),
    ],
    version=VERSION,
    step_days=STEP_DAYS,
)

claim_events = EventSource(
    table=claim_export_sq.table,
    query=Query(
        selects=selects(
            claimid="id",
            claim_number="claim_number",
            record_begin_timestamp="record_begin_timestamp",
            record_end_timestamp="record_end_timestamp",
        ),
        time_column="record_begin_timestamp",
        start_partition="2025-01-01",
    ),
)

claimcontact_events = EventSource(
    table=claimcontact_export_sq.table,
    query=Query(
        selects=selects(
            id="id",
            contactid="contactid",
            claimid="claimid",
            record_begin_timestamp="record_begin_timestamp",
            record_end_timestamp="record_end_timestamp",
        ),
        time_column="record_begin_timestamp",
        start_partition="2025-01-01",
    ),
)

contact_events = EventSource(
    table=contact_export_sq.table,
    query=Query(
        selects=selects(
            id="id",
            dateofbirth="dateofbirth",
            record_begin_timestamp="record_begin_timestamp",
            record_end_timestamp="record_end_timestamp",
        ),
        time_column="record_begin_timestamp",
        start_partition="2025-01-01",
    ),
)

claim_scd2_gb = scd2.GroupBy(
    sources=[claim_events],
    keys=["claimid"],
    row_id_column="claimid",
    valid_from_column="record_begin_timestamp",
    valid_to_column="record_end_timestamp",
    aggregations=[
        scd2.Aggregation(input_column="claim_number", operation=scd2.Operation.LATEST),
    ],
    online=False,
    version=VERSION,
    offline_schedule="@daily",
    step_days=STEP_DAYS,
)

claimcontact_scd2_gb = scd2.GroupBy(
    sources=[claimcontact_events],
    keys=["id"],
    row_id_column="id",
    valid_from_column="record_begin_timestamp",
    valid_to_column="record_end_timestamp",
    aggregations=[
        scd2.Aggregation(input_column="claimid", operation=scd2.Operation.LATEST),
        scd2.Aggregation(input_column="contactid", operation=scd2.Operation.LATEST),
    ],
    online=False,
    version=VERSION,
    offline_schedule="@daily",
    step_days=STEP_DAYS,
)

contact_demographics_scd2_gb = scd2.GroupBy(
    sources=[contact_events],
    keys=["id"],
    row_id_column="id",
    valid_from_column="record_begin_timestamp",
    valid_to_column="record_end_timestamp",
    aggregations=[
        scd2.Aggregation(input_column="dateofbirth", operation=scd2.Operation.LATEST),
    ],
    online=False,
    version=VERSION,
    offline_schedule="@daily",
    step_days=STEP_DAYS,
)

claimcontact_contact_join = Join(
    left=claimcontact_events,
    row_ids=["id"],
    right_parts=[
        JoinPart(
            group_by=contact_demographics_scd2_gb,
            key_mapping={"contactid": "id"},
            prefix="contact_",
        ),
    ],
    derivations=[
        Derivation(
            name="minor_contact_ind",
            expression="""
                CASE
                  WHEN contact__id_dateofbirth_latest IS NULL THEN CAST(0 AS BIGINT)
                  WHEN months_between(
                         to_date(from_unixtime(ts / 1000)),
                         to_date(contact__id_dateofbirth_latest)
                       ) < 216
                  THEN CAST(1 AS BIGINT)
                  ELSE CAST(0 AS BIGINT)
                END
                """,
        ),
        Derivation(name="*", expression="*"),
    ],
    online=False,
    version=VERSION,
    offline_schedule="@daily",
    step_days=STEP_DAYS,
)

claimcontact_with_age = JoinSource(
    join=claimcontact_contact_join,
    query=Query(
        selects=selects(
            id="id",
            claimid="claimid",
            contactid="contactid",
            minor_contact_ind="minor_contact_ind",
            record_begin_timestamp="record_begin_timestamp",
            record_end_timestamp="record_end_timestamp",
        ),
        time_column="ts",
        start_partition="2025-01-01",
    ),
)

minor_contacts_by_claim_gb = scd2.GroupBy(
    sources=[claimcontact_with_age],
    keys=["claimid"],
    row_id_column="id",
    valid_from_column="record_begin_timestamp",
    valid_to_column="record_end_timestamp",
    aggregations=[
        scd2.Aggregation(input_column="minor_contact_ind", operation=scd2.Operation.SUM),
    ],
    online=False,
    version=VERSION,
    offline_schedule="@daily",
    step_days=STEP_DAYS,
)

claim_spine_join = Join(
    left=claim_events,
    row_ids=["claimid"],
    right_parts=[
        JoinPart(
            group_by=claim_scd2_gb,
            key_mapping={"claimid": "claimid"},
            prefix="claim_",
        ),
        JoinPart(
            group_by=minor_contacts_by_claim_gb,
            key_mapping={"claimid": "claimid"},
            prefix="minor_contacts_",
        ),
    ],
    online=False,
    version=VERSION,
    offline_schedule="@daily",
    step_days=STEP_DAYS,
)

under18_contacts_expectations = StagingQuery(
    query=f"""
        SELECT
            claimid,
            claim_number,
            ts,
            COALESCE(minor_contacts__claimid_minor_contact_ind_sum, CAST(0 AS DOUBLE)) AS under18_contact_count,
            ds
        FROM {claim_spine_join.table}
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
        """,
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(
            table=claim_spine_join.table,
            partition_column="ds",
            start_offset=0,
            end_offset=0,
        ),
    ],
    version=VERSION,
    step_days=STEP_DAYS,
)
