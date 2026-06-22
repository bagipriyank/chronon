package ai.chronon.flink_connectors.kinesis

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{
  AvroCodec,
  AvroSerDe,
  DebeziumAvroSerDe,
  DebeziumJsonSerDe,
  DebeziumMutationMapper,
  JsonSchemaSerDe,
  LocalSchemaSerDe,
  Mutation,
  ProtobufSerDe,
  SerDe
}
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{
  GetSchemaVersionRequest,
  GetSchemaVersionResponse,
  SchemaId,
  SchemaVersionNumber
}

/** SerDe that fetches schemas from AWS Glue Schema Registry and auto-detects the format (Avro, JSON, or Protobuf).
  *
  * Configure via topic string:
  *   kinesis://stream-name/serde=glue_registry/registry_name=my-registry/schema_name=my-schema/[region=us-west-2]/[version_number=1]/[proto3_default_as_null=false]
  *
  * Parameters:
  *   - registry_name: Name of the Glue Schema Registry (required)
  *   - schema_name: Name of the schema in the registry (required)
  *   - region: AWS region (optional, uses AWS default region provider chain if not set)
  *   - version_number: Specific schema version (optional, defaults to latest)
  *   - aws_access_key_id / aws_secret_access_key: Explicit credentials (optional, both required if either is set)
  *   - proto3_default_as_null: For protobuf schemas, treat proto3 default values as null (optional, defaults to false)
  */
class GlueSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import GlueSchemaSerDe._

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private val proto3DefaultAsNull: Boolean =
    topicInfo.params.getOrElse(Proto3DefaultAsNullKey, "false").toBoolean

  private val isDebezium: Boolean =
    topicInfo.params.getOrElse(DebeziumMutationMapper.DebeziumKey, "false").toBoolean

  (topicInfo.params.get(AccessKeyIdKey), topicInfo.params.get(SecretAccessKeyKey)) match {
    case (Some(_), Some(_)) | (None, None) => // valid combinations
    case _ =>
      throw new IllegalArgumentException(
        s"Both $AccessKeyIdKey and $SecretAccessKeyKey must be provided together, or neither for default credential chain"
      )
  }

  protected[flink_connectors] def buildGlueClient(): GlueClient = {
    val clientBuilder = GlueClient.builder()

    topicInfo.params.get(RegionKey).foreach(r => clientBuilder.region(Region.of(r)))

    (topicInfo.params.get(AccessKeyIdKey), topicInfo.params.get(SecretAccessKeyKey)) match {
      case (Some(accessKeyId), Some(secretAccessKey)) =>
        val credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey)
        clientBuilder.credentialsProvider(StaticCredentialsProvider.create(credentials))
      case (None, None) =>
        clientBuilder.credentialsProvider(DefaultCredentialsProvider.builder().build())
      case _ =>
        // This case should never be reached due to validation above
        clientBuilder.credentialsProvider(DefaultCredentialsProvider.builder().build())
    }

    clientBuilder.build()
  }

  protected[flink_connectors] def localSchemaDirOverride: Option[String] = {
    val v = System.getenv(LocalSchemaSerDe.SchemaDirEnvVar)
    if (v != null && v.trim.nonEmpty) Some(v) else None
  }

  private lazy val delegate: SerDe = buildSerDe(topicInfo)

  private def buildSerDe(topicInfo: TopicInfo): SerDe = {
    // In dev environments, LOCAL_SCHEMA_DIR can be set to bypass Glue and load schemas from the local filesystem.
    // This allows using the same topic config (serde=glue_registry/...) in both dev and prod.
    localSchemaDirOverride match {
      case Some(dir) =>
        logger.info(s"${LocalSchemaSerDe.SchemaDirEnvVar} is set — using LocalSchemaSerDe instead of Glue")
        return new LocalSchemaSerDe(topicInfo, dir)
      case None =>
    }

    val schemaName =
      topicInfo.params.getOrElse(SchemaNameKey, throw new IllegalArgumentException(s"$SchemaNameKey not set"))

    val registryName =
      topicInfo.params.getOrElse(RegistryNameKey, throw new IllegalArgumentException(s"$RegistryNameKey not set"))

    logger.info(s"Fetching schema from Glue: registry=$registryName, schema=$schemaName")

    val glueClient = buildGlueClient()

    val schemaId = SchemaId
      .builder()
      .registryName(registryName)
      .schemaName(schemaName)
      .build()

    val versionNumber = topicInfo.params.get(VersionNumberKey) match {
      case Some(v) => SchemaVersionNumber.builder().versionNumber(v.toLong).build()
      case None    => SchemaVersionNumber.builder().latestVersion(true).build()
    }

    val request = GetSchemaVersionRequest
      .builder()
      .schemaId(schemaId)
      .schemaVersionNumber(versionNumber)
      .build()

    val response: GetSchemaVersionResponse =
      try {
        glueClient.getSchemaVersion(request)
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"Failed retrieving schema from Glue Schema Registry - registry: $registryName, schema: $schemaName",
            e)
      } finally {
        glueClient.close()
      }

    val format = response.dataFormat().toString
    val schemaDefinition = response.schemaDefinition()

    logger.info(s"Retrieved schema (version ${response.versionNumber()}, format=$format) from Glue")

    format match {
      case "AVRO" =>
        val avroSchema: Schema = AvroCodec.of(schemaDefinition).schema
        if (isDebezium) new DebeziumAvroSerDe(avroSchema) else new AvroSerDe(avroSchema)
      case "JSON" =>
        if (isDebezium) new DebeziumJsonSerDe(schemaDefinition, schemaName)
        else new JsonSchemaSerDe(schemaDefinition, schemaName)
      case "PROTOBUF" =>
        if (isDebezium)
          throw new IllegalArgumentException("debezium is not supported with Protobuf schemas. Use Avro / Json.")
        val protobufSchema = new ProtobufSchema(schemaDefinition)
        val descriptor = protobufSchema.toDescriptor()
        new ProtobufSerDe(descriptor, proto3DefaultAsNull)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported schema format: $other. Supported formats are AVRO, JSON, and PROTOBUF.")
    }
  }

  override def schema: StructType = delegate.schema

  override def fromBytes(bytes: Array[Byte]): Mutation = delegate.fromBytes(bytes)
}

object GlueSchemaSerDe {
  val RegionKey = "region"
  val AccessKeyIdKey = "aws_access_key_id"
  val SecretAccessKeyKey = "aws_secret_access_key"

  val RegistryNameKey = "registry_name"
  val SchemaNameKey = "schema_name"

  val VersionNumberKey = "version_number"
  val Proto3DefaultAsNullKey = "proto3_default_as_null"
}
