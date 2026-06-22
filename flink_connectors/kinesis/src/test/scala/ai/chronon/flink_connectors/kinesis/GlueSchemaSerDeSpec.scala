package ai.chronon.flink_connectors.kinesis

import ai.chronon.api.{Constants, IntType, StringType}
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, DebeziumMutationMapper}
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{DataFormat, GetSchemaVersionRequest, GetSchemaVersionResponse}

import java.nio.file.Files
import java.nio.charset.StandardCharsets

class MockGlueSchemaSerDe(topicInfo: TopicInfo, mockGlueClient: GlueClient) extends GlueSchemaSerDe(topicInfo) {
  override def buildGlueClient(): GlueClient = {
    mockGlueClient
  }
}

class MockGlueSchemaSerDeWithLocalDir(topicInfo: TopicInfo, localDir: String) extends GlueSchemaSerDe(topicInfo) {
  override def localSchemaDirOverride: Option[String] = Some(localDir)
  override def buildGlueClient(): GlueClient = throw new UnsupportedOperationException("should not call Glue")
}

class GlueSchemaSerDeSpec extends AnyFlatSpec {
  it should "fail if registry_name is not provided" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }

  it should "fail if schema_name is not provided" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry"
      ))
    val mockGlueClient = mock[GlueClient]

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }

  it should "succeed without region (uses default region provider chain)" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.AVRO)
      .schemaDefinition(avroSchemaStr)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val deSerSchema = glueSchemaSerDe.schema
    assert(deSerSchema != null)
  }

  it should "fail if credentials are partially provided" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema",
        GlueSchemaSerDe.AccessKeyIdKey -> "access-key-only"
      ))
    val mockGlueClient = mock[GlueClient]

    assertThrows[IllegalArgumentException] {
      new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    }
  }

  it should "fail if the schema is not found" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "non-existent-registry",
        GlueSchemaSerDe.SchemaNameKey -> "non-existent-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]()))
      .thenThrow(new RuntimeException("Schema not found"))

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }

  it should "succeed if the schema is found and is of type AVRO" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.AVRO)
      .schemaDefinition(avroSchemaStr)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val deSerSchema = glueSchemaSerDe.schema
    assert(deSerSchema != null)
  }

  it should "auto-detect JSON format and parse JSON Schema" in {
    val jsonSchema =
      """{
        |  "title": "login_event",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "ts": { "type": "integer" },
        |    "success": { "type": "boolean" }
        |  }
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "login_event"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.JSON)
      .schemaDefinition(jsonSchema)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 3)
    assert(schema.fields.exists(_.name == "user_id"))
    assert(schema.fields.exists(_.name == "ts"))
    assert(schema.fields.exists(_.name == "success"))
  }

  it should "deserialize JSON messages when Glue schema is JSON format" in {
    val jsonSchema =
      """{
        |  "title": "login_event",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "ts": { "type": "integer" },
        |    "score": { "type": "number" }
        |  }
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "login_event"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.JSON)
      .schemaDefinition(jsonSchema)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)

    val message = """{"user_id": "abc123", "ts": 1700000000000, "score": 0.95}"""
    val mutation = serDe.fromBytes(message.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    assert(mutation.after != null)
    assert(mutation.after(0) == "abc123")
    assert(mutation.after(1) == 1700000000000L)
    assert(mutation.after(2) == 0.95)
  }

  it should "fall back to LocalSchemaSerDe when LOCAL_SCHEMA_DIR override is set" in {
    val tmpDir = Files.createTempDirectory("glue-local-fallback-test")
    val avroSchemaStr =
      """{ "type": "record", "name": "test_schema", "fields": [ { "type": "string", "name": "id" } ] }"""
    Files.write(tmpDir.resolve("test-schema.avsc"), avroSchemaStr.getBytes(StandardCharsets.UTF_8))

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))

    val serDe = new MockGlueSchemaSerDeWithLocalDir(topicInfo, tmpDir.toString)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.exists(_.name == "id"))
  }

  it should "use DebeziumAvroSerDe via LocalSchemaSerDe when debezium=true and LOCAL_SCHEMA_DIR is set" in {
    val tmpDir = Files.createTempDirectory("glue-local-debezium-test")
    Files.write(tmpDir.resolve("cdc-schema.avsc"), debeziumEnvelopeSchemaStr.getBytes(StandardCharsets.UTF_8))

    val topicInfo = TopicInfo("cdc-stream", "kinesis", Map(
      GlueSchemaSerDe.RegistryNameKey -> "test-registry",
      GlueSchemaSerDe.SchemaNameKey -> "cdc-schema",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))

    val serDe = new MockGlueSchemaSerDeWithLocalDir(topicInfo, tmpDir.toString)
    val fields = serDe.schema.fields
    assert(fields.exists(_.name == "id"))
    assert(fields.exists(_.name == Constants.MutationTimeColumn))
    assert(fields.exists(_.name == Constants.ReversalColumn))

    val bytes = makeDebeziumEnvelopeBytes("c", None, Some(1), "Alice")
    val mutation = serDe.fromBytes(bytes)
    assert(mutation.before == null)
    assert(mutation.after != null)
    assert(mutation.after(1) == "Alice")
    assert(mutation.after(2) == (debeziumTs: java.lang.Long))
  }

  // ============== Proto3 Tests ==============

  it should "succeed if the schema is found and is of type PROTOBUF (proto3)" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestProto3 {
        |  string name = 1;
        |  int32 age = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF)
      .schemaDefinition(proto3SchemaStr)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto3 messages from Glue" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message User {
        |  string username = 1;
        |  int32 user_id = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "user-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF)
      .schemaDefinition(proto3SchemaStr)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)

    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("username"), "alice")
      .setField(descriptor.findFieldByName("user_id"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "alice")
    assert(mutation.after(1) == 42)
  }

  it should "handle proto3DefaultAsNull parameter for proto3 schemas from Glue" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin

    val topicInfoWithNull = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-defaults",
        GlueSchemaSerDe.Proto3DefaultAsNullKey -> "true"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF)
      .schemaDefinition(proto3SchemaStr)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDeWithNull = new MockGlueSchemaSerDe(topicInfoWithNull, mockGlueClient)

    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    val topicInfoWithoutNull = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-defaults",
        GlueSchemaSerDe.Proto3DefaultAsNullKey -> "false"
      ))
    val serDeWithoutNull = new MockGlueSchemaSerDe(topicInfoWithoutNull, mockGlueClient)

    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithoutNull.after(0) == "")
    assert(mutationWithoutNull.after(1) == 0)
  }

  // ============== Proto2 Tests ==============

  it should "succeed if the schema is found and is of type PROTOBUF (proto2)" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message TestProto2 {
        |  required string name = 1;
        |  optional int32 age = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema-proto2"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF)
      .schemaDefinition(proto2SchemaStr)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto2 messages with required and optional fields from Glue" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message Person {
        |  required string name = 1;
        |  optional int32 id = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "person-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF)
      .schemaDefinition(proto2SchemaStr)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)

    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "bob")
      .setField(descriptor.findFieldByName("id"), 123)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "bob")
    assert(mutation.after(1) == 123)
  }

  it should "handle proto2 unset optional fields as null from Glue" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message OptionalTest {
        |  required string name = 1;
        |  optional int32 value = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "optional-test"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF)
      .schemaDefinition(proto2SchemaStr)
      .versionNumber(1L)
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)

    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val messageWithOnlyRequired = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "test")
      .build()

    val mutation = serDe.fromBytes(messageWithOnlyRequired.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "test")
    assert(mutation.after(1) == null)
  }

  // ============== Debezium Tests ==============

  private val debeziumEnvelopeSchemaStr =
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
      |      "fields": [{"name": "ts_ms", "type": ["null", "long"], "default": null}]
      |    }},
      |    {"name": "ts_ms", "type": ["null", "long"], "default": null}
      |  ]
      |}""".stripMargin

  private val debeziumTs = 1710631967915L

  private def makeDebeziumEnvelopeBytes(op: String,
                                        beforeId: Option[Int],
                                        afterId: Option[Int],
                                        name: String): Array[Byte] = {
    val envelopeSchema = AvroCodec.of(debeziumEnvelopeSchemaStr).schema
    val valueSchema = envelopeSchema.getField("before").schema().getTypes.get(1)
    val sourceSchema = envelopeSchema.getField("source").schema()
    def makeValue(id: Int) = {
      val r = new org.apache.avro.generic.GenericData.Record(valueSchema)
      r.put("id", id)
      r.put("name", name)
      r
    }
    val src = new org.apache.avro.generic.GenericData.Record(sourceSchema)
    src.put("ts_ms", debeziumTs: java.lang.Long)
    val envelope = new org.apache.avro.generic.GenericData.Record(envelopeSchema)
    envelope.put("op", op)
    envelope.put("before", beforeId.map(makeValue).orNull)
    envelope.put("after", afterId.map(makeValue).orNull)
    envelope.put("source", src)
    envelope.put("ts_ms", null)
    AvroCodec.of(debeziumEnvelopeSchemaStr).encodeBinary(envelope)
  }

  it should "use DebeziumAvroSerDe when debezium=true and format is AVRO" in {
    val topicInfo = TopicInfo("cdc-stream", "kinesis", Map(
      GlueSchemaSerDe.RegionKey -> "us-east-1",
      GlueSchemaSerDe.RegistryNameKey -> "test-registry",
      GlueSchemaSerDe.SchemaNameKey -> "cdc-schema",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.AVRO).schemaDefinition(debeziumEnvelopeSchemaStr).build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val fields = serDe.schema.fields
    assert(fields.exists(_.name == "id"))
    assert(fields.exists(_.name == "name"))
    assert(fields.exists(_.name == Constants.MutationTimeColumn))
    assert(fields.exists(_.name == Constants.ReversalColumn))
  }

  it should "deserialize a Debezium Avro op=u envelope from Glue" in {
    val topicInfo = TopicInfo("cdc-stream", "kinesis", Map(
      GlueSchemaSerDe.RegionKey -> "us-east-1",
      GlueSchemaSerDe.RegistryNameKey -> "test-registry",
      GlueSchemaSerDe.SchemaNameKey -> "cdc-schema",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.AVRO).schemaDefinition(debeziumEnvelopeSchemaStr).build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val bytes = makeDebeziumEnvelopeBytes("u", Some(1), Some(1), "Alice")
    val mutation = serDe.fromBytes(bytes)

    assert(mutation.before != null)
    assert(mutation.after != null)
    assert(mutation.before(3) == (true: java.lang.Boolean))
    assert(mutation.after(3) == (false: java.lang.Boolean))
    assert(mutation.after(2) == (debeziumTs: java.lang.Long))
  }

  it should "use DebeziumJsonSerDe when debezium=true and format is JSON" in {
    val jsonEnvelopeSchema =
      """{
        |  "title": "Envelope",
        |  "type": "object",
        |  "properties": {
        |    "op":     {"type": "string"},
        |    "before": {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/Value"}]},
        |    "after":  {"oneOf": [{"type": "null"}, {"$ref": "#/$defs/Value"}]},
        |    "source": {"type": "object", "properties": {"ts_ms": {"type": ["integer", "null"]}}},
        |    "ts_ms":  {"type": ["integer", "null"]}
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

    val topicInfo = TopicInfo("cdc-stream", "kinesis", Map(
      GlueSchemaSerDe.RegionKey -> "us-east-1",
      GlueSchemaSerDe.RegistryNameKey -> "test-registry",
      GlueSchemaSerDe.SchemaNameKey -> "users",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.JSON).schemaDefinition(jsonEnvelopeSchema).build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    val fields = serDe.schema.fields
    assert(fields.exists(_.name == "id"))
    assert(fields.exists(_.name == Constants.MutationTimeColumn))
    assert(fields.exists(_.name == Constants.ReversalColumn))

    val payload = s"""{"op":"c","before":null,"after":{"id":7,"name":"Bob"},"source":{"ts_ms":$debeziumTs},"ts_ms":null}"""
    val mutation = serDe.fromBytes(payload.getBytes("UTF-8"))
    assert(mutation.before == null)
    assert(mutation.after != null)
    assert(mutation.after(1) == "Bob")
    assert(mutation.after(2) == (debeziumTs: java.lang.Long))
  }

  it should "throw when debezium=true and format is PROTOBUF" in {
    val proto3SchemaStr = """syntax = "proto3"; message X { string f = 1; }"""
    val topicInfo = TopicInfo("cdc-stream", "kinesis", Map(
      GlueSchemaSerDe.RegionKey -> "us-east-1",
      GlueSchemaSerDe.RegistryNameKey -> "test-registry",
      GlueSchemaSerDe.SchemaNameKey -> "test-schema",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.PROTOBUF).schemaDefinition(proto3SchemaStr).build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val serDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] { serDe.schema }
  }

  it should "fail for unknown schema formats" in {
    val topicInfo = TopicInfo("test-stream", "kinesis",
      Map(
        GlueSchemaSerDe.RegionKey -> "us-east-1",
        GlueSchemaSerDe.RegistryNameKey -> "test-registry",
        GlueSchemaSerDe.SchemaNameKey -> "test-schema"
      ))
    val mockGlueClient = mock[GlueClient]
    val response = GetSchemaVersionResponse.builder()
      .dataFormat(DataFormat.UNKNOWN_TO_SDK_VERSION)
      .schemaDefinition("{}")
      .build()
    when(mockGlueClient.getSchemaVersion(any[GetSchemaVersionRequest]())).thenReturn(response)

    val glueSchemaSerDe = new MockGlueSchemaSerDe(topicInfo, mockGlueClient)
    assertThrows[IllegalArgumentException] {
      glueSchemaSerDe.schema
    }
  }
}
