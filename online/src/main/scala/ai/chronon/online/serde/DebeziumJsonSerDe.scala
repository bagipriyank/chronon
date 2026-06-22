package ai.chronon.online.serde

import ai.chronon.api.StructType
import com.fasterxml.jackson.databind.ObjectMapper

import java.util
import scala.jdk.CollectionConverters._

/** SerDe for Debezium JSON-encoded CDC envelopes with schemas.enable=false.
  *
  * No wire format header, no embedded schema. Message shape:
  *   {"op":"u","before":{...},"after":{...},"source":{"ts_ms":1234, ...},"ts_ms":1234, ...}
  *
  * The envelope JSON Schema (describing the full Debezium envelope with op/before/after/source)
  * must be provided — typically fetched from a schema registry (Apicurio, Glue) by the calling SerDe.
  * The inner record schema is extracted from the "after" property of the envelope schema.
  *
  * @param envelopeJsonSchema  JSON Schema string describing the full Debezium envelope
  * @param schemaName          Fallback name when the inner JSON Schema has no "title" field
  */
class DebeziumJsonSerDe(envelopeJsonSchema: String, schemaName: String) extends SerDe {

  @transient private lazy val objectMapper: ThreadLocal[ObjectMapper] =
    ThreadLocal.withInitial(() => new ObjectMapper())

  lazy val innerChrononSchema: StructType = {
    val envelopeDef =
      objectMapper.get().readValue(envelopeJsonSchema, classOf[util.LinkedHashMap[String, AnyRef]])
    val innerSchemaDef = extractInnerSchema(envelopeDef)
    JsonConversions.toChrononSchema(innerSchemaDef, schemaName)
  }

  lazy val outputSchema: StructType = DebeziumMutationMapper.buildOutputSchema(innerChrononSchema)

  override def schema: StructType = outputSchema

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val envelope =
      objectMapper.get().readValue(bytes, classOf[util.LinkedHashMap[String, AnyRef]])

    val op = envelope.get("op").toString

    val mutationTs: java.lang.Long = extractMutationTs(envelope)

    val beforeMap =
      Option(envelope.get("before")).map(_.asInstanceOf[util.Map[String, AnyRef]]).orNull
    val afterMap =
      Option(envelope.get("after")).map(_.asInstanceOf[util.Map[String, AnyRef]]).orNull

    val beforeRow =
      if (beforeMap != null) JsonConversions.toChrononRow(beforeMap, innerChrononSchema) else null
    val afterRow =
      if (afterMap != null) JsonConversions.toChrononRow(afterMap, innerChrononSchema) else null

    DebeziumMutationMapper.toMutation(op, beforeRow, afterRow, mutationTs, outputSchema)
  }

  // Extracts the inner record JSON Schema from the "after" property of the envelope schema.
  // Handles both inline object schemas and $ref pointers (e.g. "#/$defs/Value" or "#/definitions/Value").
  private def extractInnerSchema(envelopeDef: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val properties = Option(envelopeDef.get("properties"))
      .map(_.asInstanceOf[util.Map[String, AnyRef]])
      .getOrElse(throw new IllegalArgumentException("Debezium envelope JSON Schema must have a 'properties' field"))

    val afterFieldSchema = Option(properties.get("after"))
      .map(_.asInstanceOf[util.Map[String, AnyRef]])
      .getOrElse(throw new IllegalArgumentException(
        "Debezium envelope JSON Schema must have an 'after' field under 'properties'"))

    // Unwrap nullable oneOf/anyOf: [{"type":"null"}, <inner>] or [{...}, {"type":"null"}]
    val unwrapped = unwrapNullable(afterFieldSchema, envelopeDef).getOrElse(afterFieldSchema)

    // Resolve $ref if present
    if (unwrapped.containsKey("$ref")) {
      resolveRef(unwrapped.get("$ref").toString, envelopeDef)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Unresolved $$ref in Debezium envelope 'after' field: '${unwrapped.get("$ref")}'"))
    } else {
      unwrapped
    }
  }

  // Unwraps {"oneOf":[{"type":"null"}, <schema>]} or {"anyOf":[...]} nullable patterns.
  // Returns Some(innerSchema) if nullable wrapping is detected, None otherwise.
  private def unwrapNullable(schema: util.Map[String, AnyRef],
                             rootDefs: util.Map[String, AnyRef]): Option[util.Map[String, AnyRef]] = {
    val combinator = Option(schema.get("oneOf")).orElse(Option(schema.get("anyOf")))
    combinator.flatMap {
      case list: util.List[_] =>
        list.asScala
          .map(_.asInstanceOf[util.Map[String, AnyRef]])
          .find(s => !Option(s.get("type")).map(_.toString).contains("null"))
          .map { candidate =>
            // Recursively unwrap if the non-null branch is itself a $ref
            if (candidate.containsKey("$ref"))
              resolveRef(candidate.get("$ref").toString, rootDefs).getOrElse(candidate)
            else
              candidate
          }
      case _ => None
    }
  }

  // Resolves "#/$defs/<name>" or "#/definitions/<name>" style $refs against the root schema map.
  private def resolveRef(ref: String, rootDefs: util.Map[String, AnyRef]): Option[util.Map[String, AnyRef]] = {
    if (!ref.startsWith("#/")) return None
    val parts = ref.stripPrefix("#/").split("/")
    var current: AnyRef = rootDefs
    for (part <- parts) {
      current = current match {
        case m: util.Map[_, _] => m.asInstanceOf[util.Map[String, AnyRef]].get(part)
        case _                 => null
      }
      if (current == null) return None
    }
    current match {
      case m: util.Map[_, _] => Some(m.asInstanceOf[util.Map[String, AnyRef]])
      case _                 => None
    }
  }

  // source.ts_ms takes precedence; falls back to top-level ts_ms.
  // Jackson may deserialise numeric values as Integer, Long, or Double depending on magnitude.
  private def extractMutationTs(envelope: util.Map[String, AnyRef]): java.lang.Long = {
    val fromSource = Option(envelope.get("source")).collect { case src: util.Map[_, _] =>
      Option(src.asInstanceOf[util.Map[String, AnyRef]].get("ts_ms"))
        .map(toLong)
        .orNull
    }.orNull

    if (fromSource != null) fromSource
    else Option(envelope.get("ts_ms")).map(toLong).orNull
  }

  private def toLong(value: Any): java.lang.Long =
    value match {
      case l: java.lang.Long    => l
      case i: java.lang.Integer => i.toLong: java.lang.Long
      case d: java.lang.Double  => d.toLong: java.lang.Long
      case s: String            => java.lang.Long.valueOf(s)
      case _                    => null
    }
}
