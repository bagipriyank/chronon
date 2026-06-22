package ai.chronon.online.serde

import ai.chronon.api.{BooleanType, Constants, LongType, StringType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DebeziumJsonSerDeSpec extends AnyFlatSpec with Matchers {

  // Full Debezium envelope JSON Schema — mirrors what a registry (Apicurio/Glue) stores.
  // The inner record schema is embedded under properties.after (same as Avro stores it in the "after" union).
  private val envelopeSchemaStr =
    """{
      |  "title": "Envelope",
      |  "type": "object",
      |  "properties": {
      |    "op":     {"type": "string"},
      |    "before": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/Value"}]},
      |    "after":  {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/Value"}]},
      |    "source": {
      |      "type": "object",
      |      "properties": {
      |        "ts_ms": {"type": ["integer", "null"]}
      |      }
      |    },
      |    "ts_ms": {"type": ["integer", "null"]}
      |  },
      |  "$defs": {
      |    "Value": {
      |      "title": "users",
      |      "type": "object",
      |      "properties": {
      |        "id":   {"type": "integer"},
      |        "name": {"type": ["string", "null"]}
      |      }
      |    }
      |  }
      |}""".stripMargin

  private val serDe = new DebeziumJsonSerDe(envelopeSchemaStr, "users")

  private val ts = 1710631967915L

  private def envelope(op: String,
                       before: String,
                       after: String,
                       sourceTsMs: Option[Long] = Some(ts),
                       topLevelTsMs: Option[Long] = None): Array[Byte] = {
    val beforePart = if (before != null) s""""before":$before""" else s""""before":null"""
    val afterPart = if (after != null) s""""after":$after""" else s""""after":null"""
    val sourcePart = sourceTsMs match {
      case Some(v) => s""""source":{"ts_ms":$v}"""
      case None    => s""""source":{"ts_ms":null}"""
    }
    val topLevelPart = topLevelTsMs match {
      case Some(v) => s""""ts_ms":$v"""
      case None    => s""""ts_ms":null"""
    }
    s"""{"op":"$op",$beforePart,$afterPart,$sourcePart,$topLevelPart}""".getBytes("UTF-8")
  }

  "schema" should "include inner record fields followed by mutation_ts and is_before" in {
    val fields = serDe.schema.fields
    fields.length shouldBe 4
    fields(0).name shouldBe "id"
    fields(0).fieldType shouldBe LongType  // JSON Schema "integer" maps to LongType
    fields(1).name shouldBe "name"
    fields(1).fieldType shouldBe StringType
    fields(2).name shouldBe Constants.MutationTimeColumn
    fields(2).fieldType shouldBe LongType
    fields(3).name shouldBe Constants.ReversalColumn
    fields(3).fieldType shouldBe BooleanType
  }

  "fromBytes (op=c)" should "produce before=null, after populated, is_before=false" in {
    val bytes = envelope("c", null, """{"id":1,"name":"Alice"}""")
    val m = serDe.fromBytes(bytes)

    m.before shouldBe null
    m.after should not be null
    m.after(0) shouldBe (1L: java.lang.Long)
    m.after(1) shouldBe "Alice"
    m.after(2) shouldBe (ts: java.lang.Long)
    m.after(3) shouldBe (false: java.lang.Boolean)
  }

  "fromBytes (op=r)" should "behave the same as op=c" in {
    val bytes = envelope("r", null, """{"id":2,"name":"Bob"}""")
    val m = serDe.fromBytes(bytes)

    m.before shouldBe null
    m.after should not be null
    m.after(3) shouldBe (false: java.lang.Boolean)
  }

  "fromBytes (op=d)" should "produce before populated with is_before=true, after=null" in {
    val bytes = envelope("d", """{"id":1,"name":"Alice"}""", null)
    val m = serDe.fromBytes(bytes)

    m.after shouldBe null
    m.before should not be null
    m.before(0) shouldBe (1L: java.lang.Long)
    m.before(1) shouldBe "Alice"
    m.before(2) shouldBe (ts: java.lang.Long)
    m.before(3) shouldBe (true: java.lang.Boolean)
  }

  "fromBytes (op=u)" should "populate both before and after with correct is_before values" in {
    val bytes = envelope("u", """{"id":1,"name":"Alice"}""", """{"id":1,"name":"Alicia"}""")
    val m = serDe.fromBytes(bytes)

    m.before should not be null
    m.after should not be null
    m.before(1) shouldBe "Alice"
    m.before(3) shouldBe (true: java.lang.Boolean)
    m.after(1) shouldBe "Alicia"
    m.after(3) shouldBe (false: java.lang.Boolean)
  }

  it should "set mutation_ts on both before and after rows" in {
    val bytes = envelope("u", """{"id":1,"name":"Old"}""", """{"id":1,"name":"New"}""")
    val m = serDe.fromBytes(bytes)
    m.before(2) shouldBe (ts: java.lang.Long)
    m.after(2) shouldBe (ts: java.lang.Long)
  }

  "fromBytes" should "extract mutation_ts from source.ts_ms" in {
    val bytes = envelope("c", null, """{"id":1,"name":"X"}""",
      sourceTsMs = Some(ts), topLevelTsMs = Some(9999L))
    val m = serDe.fromBytes(bytes)
    m.after(2) shouldBe (ts: java.lang.Long) // source.ts_ms takes precedence
  }

  it should "fall back to top-level ts_ms when source.ts_ms is null" in {
    val fallbackTs = 9876543210L
    val bytes = envelope("c", null, """{"id":1,"name":"X"}""",
      sourceTsMs = None, topLevelTsMs = Some(fallbackTs))
    val m = serDe.fromBytes(bytes)
    m.after(2) shouldBe (fallbackTs: java.lang.Long)
  }

  it should "handle null name field in inner record" in {
    val bytes = envelope("c", null, """{"id":3,"name":null}""")
    val m = serDe.fromBytes(bytes)
    m.after(1) shouldBe (null: AnyRef)
  }

  it should "coerce Jackson Integer to Long for id field" in {
    // Jackson parses small integers as Integer; JsonConversions.convertValue handles the coercion
    val bytes = envelope("c", null, """{"id":42,"name":"Test"}""")
    val m = serDe.fromBytes(bytes)
    m.after(0) shouldBe (42L: java.lang.Long)
  }

  it should "return a no-op Mutation for unrecognised ops (e.g. op=t truncate)" in {
    val bytes = envelope("t", null, null)
    val m = serDe.fromBytes(bytes)
    m.before shouldBe null
    m.after shouldBe null
  }

  it should "return a no-op Mutation for any other unknown op" in {
    val bytes = envelope("x", null, """{"id":1,"name":"X"}""")
    val m = serDe.fromBytes(bytes)
    m.before shouldBe null
    m.after shouldBe null
  }

  "DebeziumJsonSerDe with inline after schema" should "work without $ref (after defined inline)" in {
    // Some registries store the inner schema directly under after rather than via $ref
    val inlineEnvelopeSchema =
      """{
        |  "title": "Envelope",
        |  "type": "object",
        |  "properties": {
        |    "op": {"type": "string"},
        |    "before": {"oneOf": [{"type": "null"}, {
        |      "title": "users",
        |      "type": "object",
        |      "properties": {
        |        "id":   {"type": "integer"},
        |        "name": {"type": ["string", "null"]}
        |      }
        |    }]},
        |    "after": {"oneOf": [{"type": "null"}, {
        |      "title": "users",
        |      "type": "object",
        |      "properties": {
        |        "id":   {"type": "integer"},
        |        "name": {"type": ["string", "null"]}
        |      }
        |    }]},
        |    "source": {"type": "object", "properties": {"ts_ms": {"type": ["integer", "null"]}}},
        |    "ts_ms": {"type": ["integer", "null"]}
        |  }
        |}""".stripMargin

    val inlineSerDe = new DebeziumJsonSerDe(inlineEnvelopeSchema, "users")
    val fields = inlineSerDe.schema.fields
    fields.length shouldBe 4
    fields(0).name shouldBe "id"
    fields(1).name shouldBe "name"
    fields(2).name shouldBe Constants.MutationTimeColumn
    fields(3).name shouldBe Constants.ReversalColumn
  }
}
