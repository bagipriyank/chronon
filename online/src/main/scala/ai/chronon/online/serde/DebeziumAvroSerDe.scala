package ai.chronon.online.serde

import ai.chronon.api.StructType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.jdk.CollectionConverters._

/** SerDe for Debezium Avro-encoded CDC envelopes.
  *
  * Expects raw Avro bytes — wire format headers (magic byte + schema ID) must be stripped
  * upstream by the registry-backed wrappers (SchemaRegistrySerDe, ApicurioSchemaSerDe, etc.).
  *
  * The envelope schema must contain:
  *   op:     string            (required — "c", "r", "u", "d", "t")
  *   before: union[null, <inner_record>]  (nullable)
  *   after:  union[null, <inner_record>]  (nullable)
  *   source: record { ts_ms: union[null, long], ... }
  *   ts_ms:  union[null, long]  (top-level, used as fallback)
  *
  * The output schema is the inner record schema extended with:
  *   mutation_ts (Long) and is_before (Boolean) appended at the tail.
  */
class DebeziumAvroSerDe(val envelopeAvroSchema: Schema) extends SerDe {

  lazy val envelopeSchemaString: String = envelopeAvroSchema.toString

  @transient private lazy val innerAvroSchema: Schema = extractInnerSchema(envelopeAvroSchema)

  @transient private lazy val innerChrononSchema: StructType =
    AvroConversions.toChrononSchema(innerAvroSchema).asInstanceOf[StructType]

  @transient lazy val outputSchema: StructType = DebeziumMutationMapper.buildOutputSchema(innerChrononSchema)

  // Cached thread-local converter for the inner record schema.
  @transient private lazy val innerRowConverter =
    AvroConversions.genericRecordToChrononRowConverter(innerChrononSchema)

  override def schema: StructType = outputSchema

  override def fromBytes(bytes: Array[Byte]): Mutation = decodeEnvelope(bytes, None)

  def fromBytes(bytes: Array[Byte], writerSchema: Schema): Mutation =
    decodeEnvelope(bytes, Some(writerSchema.toString))

  def fromBytes(bytes: Array[Byte], writerSchemaStr: String): Mutation =
    decodeEnvelope(bytes, Some(writerSchemaStr))

  private def decodeEnvelope(bytes: Array[Byte], writerSchemaStr: Option[String]): Mutation = {
    val envelope: GenericRecord =
      AvroCodec.ofThreaded(envelopeSchemaString, writerSchemaStr).get().decode(bytes)

    // Avro Utf8 → String
    val op = envelope.get("op").toString
    val mutationTs = extractMutationTs(envelope)

    // union["null", <record>] — GenericDatumReader resolves to null or GenericRecord directly
    val beforeRecord = Option(envelope.get("before")).map(_.asInstanceOf[GenericRecord]).orNull
    val afterRecord = Option(envelope.get("after")).map(_.asInstanceOf[GenericRecord]).orNull

    val beforeRow = if (beforeRecord != null) innerRowConverter(beforeRecord) else null
    val afterRow = if (afterRecord != null) innerRowConverter(afterRecord) else null

    DebeziumMutationMapper.toMutation(op, beforeRow, afterRow, mutationTs, outputSchema)
  }

  // source.ts_ms takes precedence; falls back to top-level ts_ms.
  // GenericRecord.get() on union["null","long"] returns java.lang.Long directly after datum decode.
  private def extractMutationTs(envelope: GenericRecord): java.lang.Long = {
    Option(envelope.get("source"))
      .collect { case src: GenericRecord => Option(src.get("ts_ms")).collect { case l: java.lang.Long => l } }
      .flatten
      .orElse(Option(envelope.get("ts_ms")).collect { case l: java.lang.Long => l })
      .orNull
  }

  // Unwrap the union["null", <record>] on the "after" field to get the inner record schema.
  // "after" is used rather than "before" because "before" is null for inserts (op=c/r), so
  // Debezium guarantees "after" always carries the full inner record schema.
  private def extractInnerSchema(envelopeSchema: Schema): Schema = {
    val afterField = envelopeSchema.getField("after")
    require(afterField != null, "Debezium envelope Avro schema must have an 'after' field")
    val afterSchema = afterField.schema()
    afterSchema.getType match {
      case Schema.Type.UNION =>
        afterSchema.getTypes.asScala
          .find(_.getType != Schema.Type.NULL)
          .getOrElse(throw new IllegalArgumentException(
            "'after' field union contains only null type — cannot derive inner record schema"))
      case Schema.Type.RECORD => afterSchema
      case other =>
        throw new IllegalArgumentException(s"Unexpected 'after' field schema type: $other. Expected UNION or RECORD.")
    }
  }
}
