package ai.chronon.flink.deser

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{
  AvroConversions,
  AvroSerDe,
  DebeziumAvroSerDe,
  DebeziumMutationMapper,
  Mutation,
  ProtobufSerDe,
  SerDe
}
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

/** Schema Provider / SerDe implementation that uses the Confluent Schema Registry to fetch schemas for topics.
  * Supports both Avro and Protobuf schemas.
  *
  * Can be configured as: topic =
  * "kafka://topic-name/registry_host=host/[registry_port=port]/[registry_scheme=http]/[subject=subject]/[reader_schema_version=version]/[proto3_default_as_null=false]/[basic.auth.credentials.source=USER_INFO]"
  * Port, scheme, subject, reader schema version, and basic auth params are optional. If port is missing, we assume the
  * host is pointing to a LB address / such that forwards to the right host + port. Scheme defaults to http. Subject
  * defaults to the topic name + "-value" (based on schema registry conventions). If reader_schema_version is provided,
  * the SerDe uses that schema version as the reader schema instead of the latest schema at startup.
  *
  * For authenticated registries, set `basic.auth.credentials.source=USER_INFO` in the topic URI params and provide
  * credentials via the `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO` env var (or `basic.auth.user.info` in topic params).
  */
class SchemaRegistrySerDe(topicInfo: TopicInfo) extends SerDe {
  import SchemaRegistrySerDe._

  private val schemaRegistryHost: String =
    topicInfo.params.getOrElse(RegistryHostKey, throw new IllegalArgumentException(s"$RegistryHostKey not set"))

  // port is optional as many folks configure just the host as it's behind an LB
  private val schemaRegistryPortString: Option[String] = topicInfo.params.get(RegistryPortKey)

  // default to http if not set
  private val schemaRegistrySchemeString: String = topicInfo.params.getOrElse(RegistrySchemeKey, "http")

  private val CacheCapacity: Int = 10

  private val schemaRegistryWireFormat: Boolean =
    topicInfo.params.getOrElse(SchemaRegistryWireFormat, "true").toBoolean

  private val isDebezium: Boolean =
    topicInfo.params.getOrElse(DebeziumMutationMapper.DebeziumKey, "false").toBoolean

  private val proto3DefaultAsNull: Boolean =
    topicInfo.params.getOrElse(Proto3DefaultAsNullKey, "false").toBoolean

  protected[flink] def buildSchemaRegistryClient(schemeString: String,
                                                 registryHost: String,
                                                 maybePortString: Option[String]): SchemaRegistryClient = {
    val registryUrl = maybePortString match {
      case Some(portString) => s"$schemeString://$registryHost:$portString"
      case None             => s"$schemeString://$registryHost"
    }
    val authConfig = resolveRegistryAuthConfig(topicInfo.params, key => Option(System.getenv(key)))
    new CachedSchemaRegistryClient(registryUrl, CacheCapacity, authConfig.asJava)
  }

  @transient private lazy val schemaRegistryClient: SchemaRegistryClient =
    buildSchemaRegistryClient(schemaRegistrySchemeString, schemaRegistryHost, schemaRegistryPortString)

  @transient private lazy val delegate: SerDe = buildSerDe(topicInfo)

  private def readerSchemaVersion(value: String): Int =
    try {
      value.trim.toInt
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(s"$ReaderSchemaVersionKey must be an integer, got: $value", e)
    }

  private def readerSchema(subject: String): ParsedSchema = {
    val maybeReaderSchemaVersion = topicInfo.params.get(ReaderSchemaVersionKey).map(readerSchemaVersion)

    try {
      maybeReaderSchemaVersion match {
        case Some(version) =>
          val metadata = schemaRegistryClient.getSchemaMetadata(subject, version)
          schemaRegistryClient.getSchemaBySubjectAndId(subject, metadata.getId)
        case None =>
          val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
          schemaRegistryClient.getSchemaBySubjectAndId(subject, metadata.getId)
      }
    } catch {
      case e: RestClientException =>
        throw new IllegalArgumentException(
          s"Failed to retrieve schema details from the registry. Status: ${e.getStatus}; Error code: ${e.getErrorCode}",
          e)
      case e: Exception =>
        throw new IllegalArgumentException("Error connecting to and requesting schema details from the registry", e)
    }
  }

  private def buildSerDe(topicInfo: TopicInfo): SerDe = {
    val subject = topicInfo.params.getOrElse(RegistrySubjectKey, s"${topicInfo.name}-value")
    val parsedSchema = readerSchema(subject)

    parsedSchema.schemaType() match {
      case AvroSchema.TYPE =>
        val avroSchema = parsedSchema.asInstanceOf[AvroSchema].rawSchema()
        if (isDebezium) new DebeziumAvroSerDe(avroSchema) else new AvroSerDe(avroSchema)
      case ProtobufSchema.TYPE =>
        if (isDebezium)
          throw new IllegalArgumentException("debezium is not supported with Protobuf schemas. Use Avro / Json.")
        val protobufSchema = parsedSchema.asInstanceOf[ProtobufSchema]
        val descriptor = protobufSchema.toDescriptor()
        new ProtobufSerDe(descriptor, proto3DefaultAsNull)
      case other =>
        throw new IllegalArgumentException(s"Unsupported schema type: $other. Supported types are Avro and Protobuf.")
    }
  }

  override def schema: StructType = delegate.schema

  override def fromBytes(message: Array[Byte]): Mutation = {
    if (schemaRegistryWireFormat) {
      // Wire format: [0x00 magic byte][4-byte schema ID big-endian][payload]
      // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format
      val writerSchemaId = ByteBuffer.wrap(message, 1, 4).getInt
      val messageBytes = message.drop(5)
      delegate match {
        case debeziumAvroSerDe: DebeziumAvroSerDe =>
          val writerAvroSchema = schemaRegistryClient
            .getSchemaById(writerSchemaId)
            .asInstanceOf[AvroSchema]
            .rawSchema()
          debeziumAvroSerDe.fromBytes(messageBytes, writerAvroSchema)
        case avroSerDe: AvroSerDe =>
          val writerAvroSchema = schemaRegistryClient
            .getSchemaById(writerSchemaId)
            .asInstanceOf[AvroSchema]
            .rawSchema()
          avroSerDe.fromBytes(messageBytes, writerAvroSchema)
        case _ =>
          // Protobuf is self-describing (field tags in every message) — no schema resolution needed
          delegate.fromBytes(messageBytes)
      }
    } else {
      delegate.fromBytes(message)
    }
  }
}

object SchemaRegistrySerDe {
  val RegistryHostKey = "registry_host"
  val RegistryPortKey = "registry_port"
  val RegistrySchemeKey = "registry_scheme"
  val RegistrySubjectKey = "subject"
  val ReaderSchemaVersionKey = "reader_schema_version"
  val SchemaRegistryWireFormat = "schema_registry_wire_format"
  val Proto3DefaultAsNullKey = "proto3_default_as_null"

  val BasicAuthCredentialsSourceKey = "basic.auth.credentials.source"
  val BasicAuthUserInfoKey = "basic.auth.user.info"
  val BasicAuthUserInfoEnvVar = "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"

  /** Resolve Schema Registry auth config from topic params and environment variables.
    * If `basic.auth.credentials.source` is set but `basic.auth.user.info` is not,
    * falls back to the SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO env var.
    */
  def resolveRegistryAuthConfig(params: Map[String, String], getEnv: String => Option[String]): Map[String, String] = {
    val hasCredentialsSource = params.get(BasicAuthCredentialsSourceKey).exists(_.trim.nonEmpty)
    if (!hasCredentialsSource) return Map.empty

    val base = Map(BasicAuthCredentialsSourceKey -> params(BasicAuthCredentialsSourceKey).trim)
    if (params.get(BasicAuthUserInfoKey).exists(_.trim.nonEmpty)) {
      return base + (BasicAuthUserInfoKey -> params(BasicAuthUserInfoKey).trim)
    }

    getEnv(BasicAuthUserInfoEnvVar) match {
      case Some(userInfo) if userInfo.trim.nonEmpty => base + (BasicAuthUserInfoKey -> userInfo.trim)
      case _                                        => base
    }
  }
}
