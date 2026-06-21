package ai.chronon.online.serde

import ai.chronon.api.{BooleanType, Constants, IntType, LongType, StringType}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DebeziumAvroSerDeSpec extends AnyFlatSpec with Matchers {

  // Debezium envelope schema used throughout this spec.
  // "after" and "before" are union["null", Value], source.ts_ms is union["null", long].
  private val envelopeSchemaStr =
    """{
      |  "type": "record", "name": "Envelope", "namespace": "test",
      |  "fields": [
      |    {"name": "op", "type": "string"},
      |    {"name": "before", "type": ["null", {
      |      "type": "record", "name": "Value",
      |      "fields": [
      |        {"name": "id",   "type": "int"},
      |        {"name": "name", "type": ["null", "string"], "default": null}
      |      ]
      |    }], "default": null},
      |    {"name": "after",  "type": ["null", "test.Value"], "default": null},
      |    {"name": "source", "type": {
      |      "type": "record", "name": "Source",
      |      "fields": [
      |        {"name": "ts_ms", "type": ["null", "long"], "default": null}
      |      ]
      |    }},
      |    {"name": "ts_ms", "type": ["null", "long"], "default": null}
      |  ]
      |}""".stripMargin

  private val envelopeSchema: Schema = AvroCodec.of(envelopeSchemaStr).schema
  private val valueSchema: Schema = envelopeSchema.getField("before").schema().getTypes.get(1)
  private val sourceSchema: Schema = envelopeSchema.getField("source").schema()

  private val serDe = new DebeziumAvroSerDe(envelopeSchema)

  private val ts = 1710631967915L

  private def makeSource(tsMs: java.lang.Long): GenericData.Record = {
    val src = new GenericData.Record(sourceSchema)
    src.put("ts_ms", tsMs)
    src
  }

  private def makeValueRecord(id: Int, name: String): GenericData.Record = {
    val r = new GenericData.Record(valueSchema)
    r.put("id", id)
    r.put("name", name)
    r
  }

  private def makeEnvelope(op: String,
                           before: GenericData.Record,
                           after: GenericData.Record,
                           sourceTsMs: java.lang.Long,
                           topLevelTsMs: java.lang.Long = null): Array[Byte] = {
    val codec = AvroCodec.of(envelopeSchemaStr)
    val envelope = new GenericData.Record(envelopeSchema)
    envelope.put("op", op)
    envelope.put("before", before)
    envelope.put("after", after)
    envelope.put("source", makeSource(sourceTsMs))
    envelope.put("ts_ms", topLevelTsMs)
    codec.encodeBinary(envelope)
  }

  "schema" should "include inner record fields followed by mutation_ts and is_before" in {
    val fields = serDe.schema.fields
    fields.length shouldBe 4
    fields(0).name shouldBe "id"
    fields(0).fieldType shouldBe IntType
    fields(1).name shouldBe "name"
    fields(1).fieldType shouldBe StringType
    fields(2).name shouldBe Constants.MutationTimeColumn
    fields(2).fieldType shouldBe LongType
    fields(3).name shouldBe Constants.ReversalColumn
    fields(3).fieldType shouldBe BooleanType
  }

  "fromBytes (op=c)" should "produce before=null, after populated, is_before=false" in {
    val bytes = makeEnvelope("c", null, makeValueRecord(1, "Alice"), ts)
    val m = serDe.fromBytes(bytes)

    m.before shouldBe null
    m.after should not be null
    m.after(0) shouldBe (1: java.lang.Integer)
    m.after(1) shouldBe "Alice"
    m.after(2) shouldBe (ts: java.lang.Long) // mutation_ts
    m.after(3) shouldBe (false: java.lang.Boolean) // is_before
  }

  "fromBytes (op=r)" should "behave the same as op=c (snapshot read)" in {
    val bytes = makeEnvelope("r", null, makeValueRecord(2, "Bob"), ts)
    val m = serDe.fromBytes(bytes)

    m.before shouldBe null
    m.after should not be null
    m.after(3) shouldBe (false: java.lang.Boolean)
  }

  "fromBytes (op=d)" should "produce before populated with is_before=true, after=null" in {
    val bytes = makeEnvelope("d", makeValueRecord(1, "Alice"), null, ts)
    val m = serDe.fromBytes(bytes)

    m.after shouldBe null
    m.before should not be null
    m.before(0) shouldBe (1: java.lang.Integer)
    m.before(1) shouldBe "Alice"
    m.before(2) shouldBe (ts: java.lang.Long)
    m.before(3) shouldBe (true: java.lang.Boolean)
  }

  "fromBytes (op=u)" should "populate both before and after with correct is_before values" in {
    val bytes = makeEnvelope("u", makeValueRecord(1, "Alice"), makeValueRecord(1, "Alicia"), ts)
    val m = serDe.fromBytes(bytes)

    m.before should not be null
    m.after should not be null
    m.before(1) shouldBe "Alice"
    m.before(3) shouldBe (true: java.lang.Boolean) // before row: is_before=true
    m.after(1) shouldBe "Alicia"
    m.after(3) shouldBe (false: java.lang.Boolean) // after row: is_before=false
  }

  it should "set mutation_ts and is_before on both before and after rows" in {
    val bytes = makeEnvelope("u", makeValueRecord(1, "Old"), makeValueRecord(1, "New"), ts)
    val m = serDe.fromBytes(bytes)
    m.before(2) shouldBe (ts: java.lang.Long)
    m.before(3) shouldBe (true: java.lang.Boolean)
    m.after(2) shouldBe (ts: java.lang.Long)
    m.after(3) shouldBe (false: java.lang.Boolean)
  }

  "fromBytes" should "extract mutation_ts from source.ts_ms" in {
    val bytes = makeEnvelope("c", null, makeValueRecord(1, "X"), ts, topLevelTsMs = 9999L)
    val m = serDe.fromBytes(bytes)
    m.after(2) shouldBe (ts: java.lang.Long) // source.ts_ms takes precedence
  }

  it should "fall back to top-level ts_ms when source.ts_ms is null" in {
    val fallbackTs = 9876543210L
    val bytes = makeEnvelope("c", null, makeValueRecord(1, "X"), null, topLevelTsMs = fallbackTs)
    val m = serDe.fromBytes(bytes)
    m.after(2) shouldBe (fallbackTs: java.lang.Long)
  }

  it should "handle null name field (nullable inner string column)" in {
    val bytes = makeEnvelope("c", null, makeValueRecord(3, null), ts)
    val m = serDe.fromBytes(bytes)
    m.after(1) shouldBe (null: AnyRef)
  }

  it should "return a no-op Mutation for unrecognised ops (e.g. op=t truncate)" in {
    val bytes = makeEnvelope("t", null, null, ts)
    val m = serDe.fromBytes(bytes)
    m.before shouldBe null
    m.after shouldBe null
  }

  "fromBytes with writer schema" should "support schema evolution (writer schema string variant)" in {
    // Write with a schema that has no "name" field (older writer schema)
    val writerSchemaStr =
      """{
        |  "type": "record", "name": "Envelope", "namespace": "test",
        |  "fields": [
        |    {"name": "op", "type": "string"},
        |    {"name": "before", "type": ["null", {
        |      "type": "record", "name": "Value",
        |      "fields": [{"name": "id", "type": "int"}]
        |    }], "default": null},
        |    {"name": "after", "type": ["null", "test.Value"], "default": null},
        |    {"name": "source", "type": {
        |      "type": "record", "name": "Source",
        |      "fields": [{"name": "ts_ms", "type": ["null", "long"], "default": null}]
        |    }},
        |    {"name": "ts_ms", "type": ["null", "long"], "default": null}
        |  ]
        |}""".stripMargin

    val writerSchema = AvroCodec.of(writerSchemaStr).schema
    val writerValueSchema = writerSchema.getField("after").schema().getTypes.get(1)
    val writerSourceSchema = writerSchema.getField("source").schema()

    val codec = AvroCodec.of(writerSchemaStr)
    val envelope = new GenericData.Record(writerSchema)
    val valueRecord = new GenericData.Record(writerValueSchema)
    valueRecord.put("id", 42)
    val sourceRecord = new GenericData.Record(writerSourceSchema)
    sourceRecord.put("ts_ms", ts: java.lang.Long)
    envelope.put("op", "c")
    envelope.put("before", null)
    envelope.put("after", valueRecord)
    envelope.put("source", sourceRecord)
    envelope.put("ts_ms", null)

    val bytes = codec.encodeBinary(envelope)

    // Deserialize using the current (reader) envelope schema — "name" defaults to null
    val m = serDe.fromBytes(bytes, writerSchemaStr)
    m.after should not be null
    m.after(0) shouldBe (42: java.lang.Integer)
    m.after(1) shouldBe (null: AnyRef) // "name" not in writer schema → default null
    m.after(2) shouldBe (ts: java.lang.Long)
  }
}
