---
title: "Entity Source"
order: 4.5
---

# Entity Source

An `EntitySource` is used when your data comes from a **mutable entity table** — for example a database table that holds the current state of users, listings, products, or similar entities. Unlike an `EventSource` (which is append-only), entity records can be created, updated, and deleted over time.

When you add a `mutation_table` and `mutation_topic`, Chronon can compute features with **temporal accuracy**: feature values reflect the exact state of the entity at the requested timestamp — both for point-in-time correct training data and for accurate online serving — rather than being limited to the daily snapshot boundary.

**Choosing between EntitySource, EventSource, and SCD2:**

- Use **EntitySource** when your source of truth is a live production database table and you have (or can produce) a CDC stream in Debezium format. **Only** Debezium format is supported at this time. The raw mutations come off the wire and Chronon handles the reversal semantics.
- Use **[SCD2](./SCD2.md)** when your warehouse already contains a pre-modeled slowly-changing dimension table with explicit `valid_from` / `valid_to` interval columns.
- Use **EventSource** when your data is immutable append-only events (clicks, transactions, log lines) with no update or delete semantics.

---

## The Three Datasets

A streaming `EntitySource` is composed of three datasets, each serving a distinct role in the pipeline:

| Component | What it contains | Required for |
|---|---|---|
| `snapshot_table` | Daily `ds`-partitioned table; full entity state at end of each day | Batch backfill, always required |
| `mutation_table` | Historical mutation events in **Chronon format**; `ds`-partitioned | Temporal accuracy in batch — computing intra-day windowed aggregates |
| `mutation_topic` | Real-time CDC stream in **Debezium format** | Online serving via the Flink streaming job |

> **Note:** Currently, the `mutation_table` and the `mutation_topic` have *different schemas*. The topic carries raw Debezium envelopes (`before`/`after`/`op`/`source`), while the `mutation_table` holds flattened rows in Chronon's mutation format. A future release will allow the batch engine to consume the raw Debezium table directly, eliminating this difference. See [Converting Debezium data to the mutation table](#converting-debezium-batch-data-to-the-mutation-table) below.

---

## Snapshot Table

The snapshot table is a daily, `ds`-partitioned table that captures the full state of your entity at the end of each day. It has the same schema as the inner record inside your Debezium `before`/`after` payloads.

**Required columns:**

| Column | Type | Description |
|---|---|----|
| `ds` | string (`yyyy-MM-dd`) | Partition column                                |
| `ts` | Long (ms since epoch) | Row/event timestamp used for temporal windowing |
| *(entity fields)* | any | Fields you want to select or aggregate          |

**Example — listings snapshot:**

```
listing_id  BIGINT
ts          BIGINT        -- event time, ms since epoch
price_cents BIGINT
is_active   BOOLEAN
ds          STRING        -- partition column, e.g. '2025-01-15'
```

The `ts` column represents the **event time** you want to use for time-windowed aggregations — it is how you model "when did this entity event happen?" for feature computation purposes. For example, if you have a restaurant order where the charge occurs at 10:00 and the tip is added at 13:00, you might choose the charge time as `ts`. Chronon will then bucket the mutation into the 10:00 tile window, regardless of when the mutation physically arrived.

This matters in two ways:

- **Batch:** time windows (e.g. "sum over the last 7 days") are computed against `ts`, not `mutation_ts`.
- **Streaming (Flink):** the Flink job also windows on `ts` and by default retains up to 2 days of tile state. Mutations whose `ts` falls outside that window will be skipped in the streaming job and picked up instead in the next daily batch run.

---

## Mutation Table

The mutation table holds the full history of entity changes over time, partitioned by `ds`. Each row represents one side of one mutation. Its schema is the snapshot table's columns plus two additional columns.

**Required columns:**

| Column | Type | Description |
|---|---|---|
| *(all snapshot columns)* | — | Same fields as the snapshot table, including `ts` |
| `mutation_ts` | Long (ms since epoch) | When the mutation occurred; maps to `source.ts_ms` from Debezium. Must be `>= ts`. |
| `is_before` | Boolean | `true` = pre-image (before the change); `false` = post-image (after the change) |
| `ds` | string | Partition column |

**Row semantics:**

| Debezium `op` | Rows produced in mutation table |
|---|---|
| `c` (create) / `r` (snapshot read) | One row: `is_before=false` (the new entity state) |
| `u` (update) | Two rows: `is_before=true` (old values) + `is_before=false` (new values) |
| `d` (delete) | One row: `is_before=true` (the deleted entity state) |

The two-row representation of updates lets Chronon correctly reverse old aggregation contributions (the `is_before=true` row subtracts) and apply the new ones (the `is_before=false` row adds).

**Why is the mutation table needed if we have a streaming topic?**

For `Accuracy.TEMPORAL` GroupBys, the batch `GroupByUpload` job must compute windowed aggregates accurate to the milli, not just the day. The daily snapshot table only gives day-boundary precision. The mutation table fills in the intra-day history so that the batch IR is correct before streaming tiles are merged on top.

> **Column name defaults:** Chronon looks for `mutation_ts` and `is_before` by default. If your table uses different column names, override them with `mutation_time_column` and `reversal_column` in the `Query` (see [GroupBy definition](#groupby-definition)).

---

## Converting Debezium Batch Data to the Mutation Table

If your raw mutation history is stored as Debezium-format records (with `before`/`after`/`op`/`source` envelope columns), use a `StagingQuery` to transform it into the Chronon mutation table format. This is currently a required step; a future Chronon release will allow the batch engine to read raw Debezium data directly.

```python
from ai.chronon.staging_query import StagingQuery
from ai.chronon.utils import MetaData

mutations_sq = StagingQuery(
    query="""
        -- After-image rows: inserts (op='c'/'r') and update post-images (op='u')
        SELECT after.*, false AS is_before,
               source.ts_ms AS mutation_ts,
               DATE(FROM_UNIXTIME(source.ts_ms / 1000)) AS ds
        FROM   cdc_raw
        WHERE  op IN ('c', 'r', 'u')
          AND  ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'

        UNION ALL

        -- Before-image rows: deletes (op='d') and update pre-images (op='u')
        SELECT before.*, true AS is_before,
               source.ts_ms AS mutation_ts,
               DATE(FROM_UNIXTIME(source.ts_ms / 1000)) AS ds
        FROM   cdc_raw
        WHERE  op IN ('u', 'd')
          AND  ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    """,
    output_namespace="your_namespace",
    metaData=MetaData(
        name="your_entity.mutations",
        dependencies=["cdc_raw"],
    ),
)
```

Then reference `mutations_sq.table` as the `mutation_table` in your `EntitySource`.

> The `cdc_raw` table above is your raw Debezium events table. Replace it with the actual table name in your warehouse. The `{{ start_date }}` / `{{ end_date }}` placeholders are filled in by Chronon's orchestration framework.

---

## Mutation Topic: Debezium Format

The `mutation_topic` carries real-time CDC events in Debezium format. Chronon uses only the following fields from the Debezium envelope — all other fields (transaction metadata, connector metadata, etc.) are ignored and can be omitted:

| Field | Type | How Chronon uses it |
|---|---|---|
| `op` | string | Determines mutation type: `"c"`, `"r"`, `"u"`, `"d"` |
| `before` | object or null | Pre-image of the entity row; null for creates |
| `after` | object or null | Post-image of the entity row; null for deletes |
| `source.ts_ms` | long (ms) | Mutation timestamp → `mutation_ts`. Takes precedence over top-level `ts_ms`. |
| `ts_ms` | long (ms) | Top-level fallback mutation timestamp when `source.ts_ms` is absent |

The inner record inside `before`/`after` carries your entity fields (the same fields as your snapshot table, including `ts`). Chronon extracts these fields and appends `mutation_ts` and `is_before` internally. You  **do not** need to include `mutation_ts` or `is_before` inside the `before`/`after` structs.

**Minimal Debezium JSON event:**

```json
{
  "op": "u",
  "before": {
    "listing_id": 42,
    "ts": 1717000000000,
    "price_cents": 1500,
    "is_active": true
  },
  "after": {
    "listing_id": 42,
    "ts": 1717000000000,
    "price_cents": 1800,
    "is_active": true
  },
  "source": {
    "ts_ms": 1717003600000
  }
}
```

Both JSON (`serde=json`) and Avro (`serde=avro`) serialization formats are supported, specified in the topic string. For Avro, set `serde=avro` and configure your schema registry details (see below).

---

## Schema Registry (Avro / JSON Schema)

When using a schema registry (AWS Glue, Apicurio, Confluent), register the **Debezium envelope schema** — not the inner record schema alone. The topic string specifies the registry and schema name; Chronon fetches the schema at runtime to deserialize events.

**Key requirement:** `before` and `after` must reference the *same* inner record definition. Use `$ref` / `$defs` (JSON Schema) or a named type reference (Avro) to avoid duplicating the inner schema.

**Minimal JSON Schema for the envelope:**

```json
{
  "title": "Envelope",
  "type": "object",
  "properties": {
    "op": { "type": "string" },
    "before": {
      "oneOf": [
        { "type": "null" },
        { "$ref": "#/$defs/Value" }
      ]
    },
    "after": {
      "oneOf": [
        { "type": "null" },
        { "$ref": "#/$defs/Value" }
      ]
    },
    "source": {
      "type": "object",
      "properties": {
        "ts_ms": { "type": ["integer", "null"] }
      }
    },
    "ts_ms": { "type": ["integer", "null"] }
  },
  "$defs": {
    "Value": {
      "title": "listing",
      "type": "object",
      "properties": {
        "listing_id":  { "type": ["null", "integer"], "default": null },
        "ts":          { "type": "integer" },
        "price_cents": { "type": ["null", "integer"], "default": null },
        "is_active":   { "type": ["null", "boolean"], "default": null }
      }
    }
  }
}
```

> `source.ts_ms` is the mutation time and must be `>=` the `ts` field inside `before`/`after`. If `source.ts_ms` is null or absent, Chronon falls back to the top-level `ts_ms`.

---

## GroupBy Definition

With the three data datasets in place, define your `EntitySource` and `GroupBy`:

```python
from ai.chronon.types import Accuracy, EntitySource, GroupBy, Query, selects

source = EntitySource(
    snapshot_table="demo.dim_listings_snapshot",
    mutation_table=mutations_sq.table,   # or a string like "demo.dim_listings_mutations"
    mutation_topic="kinesis://dim-listings-mutations/serde=glue_registry/registry_name=my-registry/schema_name=dim-listings-envelope",
    query=Query(
        selects=selects(
            listing_id="listing_id",
            price_cents="price_cents",
            is_active="is_active",
        ),
        start_partition="2025-01-01",
        time_column="ts",
        # Override these only if your mutation table uses non-default column names:
        # mutation_time_column="mutation_ts",   # default: "mutation_ts"
        # reversal_column="is_before",           # default: "is_before"
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["listing_id"],
    accuracy=Accuracy.TEMPORAL,
    aggregations=[...],
)
```

**Key `Query` parameters for EntitySource:**

| Parameter | Default | Description |
|---|---|---|
| `time_column` | *(required)* | The entity timestamp field inside `before`/`after`, in ms since epoch |
| `mutation_time_column` | `"mutation_ts"` | Column in the mutation table holding the mutation timestamp |
| `reversal_column` | `"is_before"` | Column in the mutation table holding the before/after flag |

The `selects` map refers to fields inside the inner record (i.e. inside `before`/`after`), not to the top-level Debezium envelope fields. Derived expressions (like `IF(price_cents > 10000, 1, 0)`) are valid Spark SQL.

---

## FAQ

**Q: Can I skip the `mutation_table` if I only care about online feature serving?**

Not for `Accuracy.TEMPORAL`. The batch `GroupByUpload` job requires the mutation table to compute intra-day tail hops and the collapsed IR. Without it, the batch IR is only accurate to day boundaries. If sub-day accuracy is not needed, you can use `Accuracy.SNAPSHOT` and omit both `mutation_table` and `mutation_topic`.

---

**Q: Can I point `mutation_table` directly at my raw Debezium events table?**

Not currently. The Chronon batch engine expects the mutation table to be in the flattened Chronon format (snapshot columns + `mutation_ts` + `is_before`). Raw Debezium tables have a nested `before`/`after` structure that the batch engine does not yet read natively. Use the [StagingQuery pattern](#converting-debezium-batch-data-to-the-mutation-table) to transform your raw Debezium table. Support for reading raw Debezium tables directly in batch is planned for a future release.

---

**Q: What Debezium fields does Chronon actually use? Can I strip the rest?**

Chronon uses only `op`, `before`, `after`, `source.ts_ms` (with `ts_ms` as a fallback). All other Debezium metadata — `transaction`, `schema`, connector info, etc. — is ignored. You can omit those fields from your topic events and schema registry definition. The minimal envelope is: `op` + `before` + `after` + `source.ts_ms`.

---

**Q: Does the schema registered with the schema registry need to match the mutation table schema?**

No. The schema registry is used **only on the streaming/Flink side** to deserialize topic events. It should match the Debezium envelope format (with `before`/`after`/`op`/`source`). The mutation table is a separate table with a flat schema and is read by the batch engine independently.
