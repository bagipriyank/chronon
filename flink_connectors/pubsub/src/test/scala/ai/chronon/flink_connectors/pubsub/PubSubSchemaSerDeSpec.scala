package ai.chronon.flink_connectors.pubsub

import ai.chronon.api.{Constants, IntType, StringType}
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, DebeziumMutationMapper}
import com.google.api.gax.rpc.{NotFoundException, StatusCode}
import com.google.cloud.pubsub.v1.SchemaServiceClient
import com.google.protobuf.DynamicMessage
import com.google.pubsub.v1.{Schema, SchemaName}
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec

class MockPubSubSchemaSerDe(topicInfo: TopicInfo, mockSchemaClient: SchemaServiceClient) extends PubSubSchemaSerDe(topicInfo) {
  override def buildPubsubSchemaClient(): SchemaServiceClient = {
    mockSchemaClient
  }
}

class PubSubSchemaSerDeSpec extends AnyFlatSpec {
  it should "fail if the schema is not found" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val statusCode = mock[StatusCode]
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenThrow(new NotFoundException(new IllegalArgumentException(), statusCode, true))

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assertThrows[IllegalArgumentException] {
      pubSubSchemaSerDe.schema
    }
  }

  it should "fail if the schema type is unsupported" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.TYPE_UNSPECIFIED).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assertThrows[IllegalArgumentException] {
      pubSubSchemaSerDe.schema
    }
  }

  it should "succeed if the schema is found and is of type AVRO" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.AVRO).setDefinition(avroSchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val deSerSchema = pubSubSchemaSerDe.schema
    assert(deSerSchema != null)
  }

  // ============== Proto3 Tests ==============

  it should "succeed if the schema is found and is of type PROTOCOL_BUFFER (proto3)" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestProto3 {
        |  string name = 1;
        |  int32 age = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("test-schema")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto3SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val chrononSchema = serDe.schema
    assert(chrononSchema != null)
    assert(chrononSchema.fields.length == 2)
    assert(chrononSchema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(chrononSchema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto3 messages from PubSub" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message User {
        |  string username = 1;
        |  int32 user_id = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "user-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("user-schema")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto3SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

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

  it should "handle proto3DefaultAsNull parameter for proto3 schemas from PubSub" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin

    val topicInfoWithNull = TopicInfo("test-topic", "pubsub",
      Map(
        PubSubSchemaSerDe.ProjectKey -> "test-project",
        PubSubSchemaSerDe.SchemaIdKey -> "test-defaults",
        PubSubSchemaSerDe.Proto3DefaultAsNullKey -> "true"
      ))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("test-defaults")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto3SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDeWithNull = new MockPubSubSchemaSerDe(topicInfoWithNull, mockedSchemaClient)

    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    val descriptor = protobufSchema.toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    val topicInfoWithoutNull = TopicInfo("test-topic", "pubsub",
      Map(
        PubSubSchemaSerDe.ProjectKey -> "test-project",
        PubSubSchemaSerDe.SchemaIdKey -> "test-defaults",
        PubSubSchemaSerDe.Proto3DefaultAsNullKey -> "false"
      ))
    val serDeWithoutNull = new MockPubSubSchemaSerDe(topicInfoWithoutNull, mockedSchemaClient)

    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithoutNull.after(0) == "")
    assert(mutationWithoutNull.after(1) == 0)
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

  it should "use DebeziumAvroSerDe when debezium=true and schema type is AVRO" in {
    val topicInfo = TopicInfo("cdc-topic", "pubsub", Map(
      PubSubSchemaSerDe.ProjectKey -> "test-project",
      PubSubSchemaSerDe.SchemaIdKey -> "cdc-schema",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("cdc-schema").setType(Schema.Type.AVRO).setDefinition(debeziumEnvelopeSchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val fields = serDe.schema.fields
    assert(fields.exists(_.name == "id"))
    assert(fields.exists(_.name == "name"))
    assert(fields.exists(_.name == Constants.MutationTimeColumn))
    assert(fields.exists(_.name == Constants.ReversalColumn))
  }

  it should "deserialize a Debezium Avro op=c envelope from PubSub" in {
    val topicInfo = TopicInfo("cdc-topic", "pubsub", Map(
      PubSubSchemaSerDe.ProjectKey -> "test-project",
      PubSubSchemaSerDe.SchemaIdKey -> "cdc-schema",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val pubsubSchema = Schema.newBuilder()
      .setName("cdc-schema").setType(Schema.Type.AVRO).setDefinition(debeziumEnvelopeSchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(pubsubSchema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

    val envelopeSchema = AvroCodec.of(debeziumEnvelopeSchemaStr).schema
    val valueSchema = envelopeSchema.getField("before").schema().getTypes.get(1)
    val sourceSchema = envelopeSchema.getField("source").schema()
    val value = new org.apache.avro.generic.GenericData.Record(valueSchema)
    value.put("id", 5)
    value.put("name", "Carol")
    val src = new org.apache.avro.generic.GenericData.Record(sourceSchema)
    src.put("ts_ms", debeziumTs: java.lang.Long)
    val envelope = new org.apache.avro.generic.GenericData.Record(envelopeSchema)
    envelope.put("op", "c")
    envelope.put("before", null)
    envelope.put("after", value)
    envelope.put("source", src)
    envelope.put("ts_ms", null)
    val bytes = AvroCodec.of(debeziumEnvelopeSchemaStr).encodeBinary(envelope)

    val mutation = serDe.fromBytes(bytes)
    assert(mutation.before == null)
    assert(mutation.after != null)
    assert(mutation.after(1) == "Carol")
    assert(mutation.after(2) == (debeziumTs: java.lang.Long))
    assert(mutation.after(3) == (false: java.lang.Boolean))
  }

  it should "throw when debezium=true and schema type is PROTOCOL_BUFFER" in {
    val proto3SchemaStr = """syntax = "proto3"; message X { string f = 1; }"""
    val topicInfo = TopicInfo("cdc-topic", "pubsub", Map(
      PubSubSchemaSerDe.ProjectKey -> "test-project",
      PubSubSchemaSerDe.SchemaIdKey -> "cdc-schema",
      DebeziumMutationMapper.DebeziumKey -> "true"
    ))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("cdc-schema").setType(Schema.Type.PROTOCOL_BUFFER).setDefinition(proto3SchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assertThrows[IllegalArgumentException] { serDe.schema }
  }

  // ============== Proto2 Tests ==============

  it should "succeed if the schema is found and is of type PROTOCOL_BUFFER (proto2)" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message TestProto2 {
        |  required string name = 1;
        |  optional int32 age = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema-proto2"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("test-schema-proto2")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto2SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val chrononSchema = serDe.schema
    assert(chrononSchema != null)
    assert(chrononSchema.fields.length == 2)
    assert(chrononSchema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(chrononSchema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto2 messages with required and optional fields from PubSub" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message Person {
        |  required string name = 1;
        |  optional int32 id = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "person-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("person-schema")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto2SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

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

  it should "handle proto2 unset optional fields as null from PubSub" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message OptionalTest {
        |  required string name = 1;
        |  optional int32 value = 2;
        |}""".stripMargin

    val topicInfo = TopicInfo("test-topic", "pubsub",
      Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "optional-test"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder()
      .setName("optional-test")
      .setType(Schema.Type.PROTOCOL_BUFFER)
      .setDefinition(proto2SchemaStr)
      .build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val serDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)

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
}
