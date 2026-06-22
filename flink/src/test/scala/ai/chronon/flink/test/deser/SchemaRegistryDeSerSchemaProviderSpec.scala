package ai.chronon.flink.test.deser

import ai.chronon.api.{BooleanType, Constants, IntType, LongType, StringType, StructField}
import ai.chronon.flink.deser.SchemaRegistrySerDe
import ai.chronon.flink.deser.SchemaRegistrySerDe._
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, DebeziumAvroSerDe, DebeziumMutationMapper}
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.schemaregistry.SchemaProvider
import io.confluent.kafka.schemaregistry.avro.{AvroSchema, AvroSchemaProvider}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.{ProtobufSchema, ProtobufSchemaProvider}
import org.apache.avro.generic.GenericData
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

class MockSchemaRegistrySerDe(topicInfo: TopicInfo,
                             mockSchemaRegistryClient: MockSchemaRegistryClient)
    extends SchemaRegistrySerDe(topicInfo) {
  override def buildSchemaRegistryClient(schemeString: String,
                                         registryHost: String,
                                         maybePortString: Option[String]): MockSchemaRegistryClient =
    mockSchemaRegistryClient
}

class SchemaRegistrySerDeSpec extends AnyFlatSpec {

  private val avroSchemaProvider: SchemaProvider = new AvroSchemaProvider
  private val protoSchemaProvider: SchemaProvider = new ProtobufSchemaProvider
  val schemaRegistryClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider, protoSchemaProvider).asJava)

  it should "fail if the schema subject is not found" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.schema
    }
  }

  it should "succeed if we look up an avro schema that is present" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("test-topic-avro-value", new AvroSchema(avroSchemaStr))
    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  it should "succeed if we look up an avro schema using injected subject" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("my-subject", new AvroSchema(avroSchemaStr))

    val topicInfo = TopicInfo("another-topic", "kafka", Map(RegistryHostKey -> "localhost", "subject" -> "my-subject"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  // ============== Proto3 Tests ==============

  it should "succeed if we look up a proto3 schema" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestProto3 {
        |  string name = 1;
        |  int32 age = 2;
        |}""".stripMargin
    schemaRegistryClient.register("test-topic-proto3-value", new ProtobufSchema(proto3SchemaStr))

    val topicInfo = TopicInfo("test-topic-proto3", "kafka", Map(RegistryHostKey -> "localhost"))
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto3 messages" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message User {
        |  string username = 1;
        |  int32 user_id = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-topic-proto3-deser-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-topic-proto3-deser",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

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

  it should "handle proto3DefaultAsNull parameter for proto3 schemas" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-proto3-defaults-value", protobufSchema)

    val topicInfoWithNull = TopicInfo(
      "test-proto3-defaults",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false", Proto3DefaultAsNullKey -> "true")
    )
    val serDeWithNull = new MockSchemaRegistrySerDe(topicInfoWithNull, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    val topicInfoWithoutNull = TopicInfo(
      "test-proto3-defaults",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false", Proto3DefaultAsNullKey -> "false")
    )
    val serDeWithoutNull = new MockSchemaRegistrySerDe(topicInfoWithoutNull, schemaRegistryClient)

    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithoutNull.after(0) == "")
    assert(mutationWithoutNull.after(1) == 0)
  }

  it should "handle wire format with 5-byte header for proto3" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message WireFormatTest {
        |  string value = 1;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-wire-format-proto3-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-wire-format-proto3",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("value"), "test")
      .build()

    val wireFormatBytes = Array[Byte](0x00, 0x00, 0x00, 0x00, 0x01) ++ message.toByteArray

    val mutation = serDe.fromBytes(wireFormatBytes)
    assert(mutation.after != null)
    assert(mutation.after(0) == "test")
  }

  // ============== Proto2 Tests ==============

  it should "succeed if we look up a proto2 schema" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message TestProto2 {
        |  required string name = 1;
        |  optional int32 age = 2;
        |}""".stripMargin
    schemaRegistryClient.register("test-topic-proto2-value", new ProtobufSchema(proto2SchemaStr))

    val topicInfo = TopicInfo("test-topic-proto2", "kafka", Map(RegistryHostKey -> "localhost"))
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto2 messages with required and optional fields" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message Person {
        |  required string name = 1;
        |  optional int32 id = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    schemaRegistryClient.register("test-topic-proto2-deser-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-topic-proto2-deser",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

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

  it should "handle proto2 unset optional fields as null" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message OptionalTest {
        |  required string name = 1;
        |  optional int32 value = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    schemaRegistryClient.register("test-proto2-optional-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-proto2-optional",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

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

  "resolveRegistryAuthConfig" should "return empty map when no basic.auth.credentials.source is set" in {
    val params = Map(RegistryHostKey -> "localhost")
    val result = SchemaRegistrySerDe.resolveRegistryAuthConfig(params, _ => None)
    assert(result.isEmpty)
  }

  it should "inject basic.auth.user.info from env var when credentials source is set but user info is not" in {
    val params = Map(
      RegistryHostKey -> "localhost",
      BasicAuthCredentialsSourceKey -> "USER_INFO"
    )
    val result = SchemaRegistrySerDe.resolveRegistryAuthConfig(params, {
      case BasicAuthUserInfoEnvVar => Some("my-key:my-secret")
      case _                      => None
    })
    assert(result(BasicAuthCredentialsSourceKey) == "USER_INFO")
    assert(result(BasicAuthUserInfoKey) == "my-key:my-secret")
  }

  it should "use explicit basic.auth.user.info from params over env var" in {
    val params = Map(
      RegistryHostKey -> "localhost",
      BasicAuthCredentialsSourceKey -> "USER_INFO",
      BasicAuthUserInfoKey -> "explicit-key:explicit-secret"
    )
    val result = SchemaRegistrySerDe.resolveRegistryAuthConfig(params, _ => Some("should-not-be-used"))
    assert(result(BasicAuthUserInfoKey) == "explicit-key:explicit-secret")
  }

  it should "return only credentials source when env var is missing and user info is not in params" in {
    val params = Map(
      RegistryHostKey -> "localhost",
      BasicAuthCredentialsSourceKey -> "USER_INFO"
    )
    val result = SchemaRegistrySerDe.resolveRegistryAuthConfig(params, _ => None)
    assert(result(BasicAuthCredentialsSourceKey) == "USER_INFO")
    assert(!result.contains(BasicAuthUserInfoKey))
  }

  it should "return empty map when credentials source is empty string" in {
    val params = Map(
      RegistryHostKey -> "localhost",
      BasicAuthCredentialsSourceKey -> "  "
    )
    val result = SchemaRegistrySerDe.resolveRegistryAuthConfig(params, _ => Some("my-key:my-secret"))
    assert(result.isEmpty)
  }

  it should "skip env var when it is blank" in {
    val params = Map(
      RegistryHostKey -> "localhost",
      BasicAuthCredentialsSourceKey -> "USER_INFO"
    )
    val result = SchemaRegistrySerDe.resolveRegistryAuthConfig(params, _ => Some("  "))
    assert(result(BasicAuthCredentialsSourceKey) == "USER_INFO")
    assert(!result.contains(BasicAuthUserInfoKey))
  }

  // ============== Schema Evolution Bug Tests ==============

  /**
    * Helper to build a Confluent wire format message:
    * [0x00 magic byte] [4-byte schema ID big-endian] [avro payload]
    */
  private def buildWireFormatMessage(schemaId: Int, avroPayload: Array[Byte]): Array[Byte] = {
    val header = ByteBuffer.allocate(5)
    header.put(0x00.toByte) // magic byte
    header.putInt(schemaId)
    header.array() ++ avroPayload
  }

  // Schema evolution scenario 1: Reading historical data
  // Topic has records written with schema 1 (old) and schema 2 (new).
  // SchemaRegistrySerDe fetches schema 2 (latest) as the reader schema but reads the writer
  // schema ID from the wire header to correctly decode schema-1 messages.
  it should "correctly decode old data (schema 1) when latest schema (schema 2) adds a nullable field" in {
    val schema1Str =
      """{ "type": "record", "name": "User", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val schema2Str =
      """{ "type": "record", "name": "User", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" },
        |  { "name": "email", "type": ["null", "string"], "default": null }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-1-value"

    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))
    val schema2Id = freshClient.register(subject, new AvroSchema(schema2Str))
    assert(schema1Id != schema2Id, "Schema IDs should differ")

    val codec1 = new AvroCodec(schema1Str)
    val record1 = new GenericData.Record(codec1.schema)
    record1.put("name", "John")
    record1.put("age", 30)
    val wireMessage = buildWireFormatMessage(schema1Id, codec1.encodeBinary(record1))

    val topicInfo = TopicInfo(
      "evolution-test-1",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // Avro resolution: schema-1 bytes decoded with schema-2 as reader.
    // The missing "email" field is filled with its default (null).
    val mutation = serDe.fromBytes(wireMessage)
    assert(mutation.after(0) == "John")
    assert(mutation.after(1) == 30)
    assert(mutation.after(2) == null, "email should be null (default from schema 2)")
  }

  it should "decode mixed writer versions using a pinned reader schema version when forward compatible schemas are used" in {
    val schema1Str =
      """{ "type": "record", "name": "ForwardCompatibleEvent", "fields": [
        |  { "name": "entityId", "type": "string" },
        |  { "name": "userId", "type": "string" },
        |  { "name": "ts", "type": "long" },
        |  { "name": "eventCount", "type": "int" }
        |]}""".stripMargin

    val schema2Str =
      """{ "type": "record", "name": "ForwardCompatibleEvent", "fields": [
        |  { "name": "entityId", "type": "string" },
        |  { "name": "userId", "type": "string" },
        |  { "name": "ts", "type": "long" },
        |  { "name": "eventCount", "type": "int" },
        |  { "name": "newStringField", "type": "string" },
        |  { "name": "newIntField", "type": "int" }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "forward-compatible-version-pin-test-value"

    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))
    val schema2Id = freshClient.register(subject, new AvroSchema(schema2Str))
    assert(schema1Id != schema2Id, "Schema IDs should differ")

    val topicInfo = TopicInfo(
      "forward-compatible-version-pin-test",
      "kafka",
      Map(
        RegistryHostKey -> "localhost",
        SchemaRegistryWireFormat -> "true",
        ReaderSchemaVersionKey -> "1"
      )
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    assert(serDe.schema.fields.map(_.name).toSeq == Seq("entityId", "userId", "ts", "eventCount"))

    val codec1 = new AvroCodec(schema1Str)
    val record1 = new GenericData.Record(codec1.schema)
    record1.put("entityId", "e1")
    record1.put("userId", "u1")
    record1.put("ts", 1234L)
    record1.put("eventCount", 1)

    val codec2 = new AvroCodec(schema2Str)
    val record2 = new GenericData.Record(codec2.schema)
    record2.put("entityId", "e2")
    record2.put("userId", "u2")
    record2.put("ts", 5678L)
    record2.put("eventCount", 2)
    record2.put("newStringField", "new-value")
    record2.put("newIntField", 1000)

    val mutation1 = serDe.fromBytes(buildWireFormatMessage(schema1Id, codec1.encodeBinary(record1)))
    assert(mutation1.after.toSeq == Seq("e1", "u1", 1234L, 1))

    val mutation2 = serDe.fromBytes(buildWireFormatMessage(schema2Id, codec2.encodeBinary(record2)))
    assert(mutation2.after.toSeq == Seq("e2", "u2", 5678L, 2))
  }

  it should "fail when reader schema version is not registered" in {
    val schemaStr =
      """{ "type": "record", "name": "MissingReaderVersion", "fields": [
        |  { "name": "id", "type": "string" }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    freshClient.register("missing-reader-version-test-value", new AvroSchema(schemaStr))

    val topicInfo = TopicInfo(
      "missing-reader-version-test",
      "kafka",
      Map(
        RegistryHostKey -> "localhost",
        ReaderSchemaVersionKey -> "2"
      )
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    val thrown = intercept[IllegalArgumentException] {
      serDe.schema
    }
    assert(thrown.getMessage.contains("Failed to retrieve schema details from the registry"))
  }

  // Schema evolution scenario 2: Flink started before schema upgrade
  // Flink starts and caches schema 1 as the reader (latest at startup).
  // Later, producers upgrade to schema 2. The wire header carries schema 2's ID,
  // so the SerDe fetches schema 2 as the writer schema and decodes correctly.
  it should "correctly decode new data (schema 2) when Flink started with schema 1 as reader" in {
    val schema1Str =
      """{ "type": "record", "name": "Person", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val schema2Str =
      """{ "type": "record", "name": "Person", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" },
        |  { "name": "email", "type": ["null", "string"], "default": null }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-2-value"
    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))

    val topicInfo = TopicInfo(
      "evolution-test-2",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // Force initialization — SerDe caches schema 1 as the reader schema
    assert(serDe.schema != null)

    // Producer upgrades to schema 2 and writes a new record
    val codec2 = new AvroCodec(schema2Str)
    val record2 = new GenericData.Record(codec2.schema)
    record2.put("name", "Alice")
    record2.put("age", 25)
    record2.put("email", "alice@test.com")
    val schema2Id = freshClient.register(subject, new AvroSchema(schema2Str))
    val wireMessage = buildWireFormatMessage(schema2Id, codec2.encodeBinary(record2))

    // Avro resolution: schema-2 bytes decoded with schema-1 as reader.
    // The extra "email" field in the writer schema is ignored (not in reader schema).
    val mutation = serDe.fromBytes(wireMessage)
    assert(mutation.after(0) == "Alice")
    assert(mutation.after(1) == 25)
  }

  // Schema evolution scenario 3: new field inserted in the MIDDLE (incompatible change)
  // Flink starts and caches schema 1 as the reader. Producer registers schema 3 which
  // inserts "email" between "name" and "age". Because the wire header carries schema 3's ID,
  // the SerDe fetches schema 3 as the writer and uses Avro resolution to map fields by name,
  // correctly extracting "name" and "age" despite the positional shift.
  it should "correctly decode data when a new field is inserted in the middle (schema 3) and Flink has schema 1 as reader" in {
    val schema1Str =
      """{ "type": "record", "name": "Employee", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val schema3Str =
      """{ "type": "record", "name": "Employee", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "email", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-3-value"
    freshClient.register(subject, new AvroSchema(schema1Str))

    val topicInfo = TopicInfo(
      "evolution-test-3",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    assert(serDe.schema != null)

    val codec3 = new AvroCodec(schema3Str)
    val record3 = new GenericData.Record(codec3.schema)
    record3.put("name", "Bob")
    record3.put("email", "bob@test.com")
    record3.put("age", 35)

    // MockSchemaRegistryClient doesn't enforce compatibility by default
    val schema3Id = freshClient.register(subject, new AvroSchema(schema3Str))
    val wireMessage = buildWireFormatMessage(schema3Id, codec3.encodeBinary(record3))

    // Avro resolution maps fields by name: "name" and "age" are matched correctly,
    // "email" (present in writer, absent in reader) is skipped.
    val mutation = serDe.fromBytes(wireMessage)
    assert(mutation.after(0) == "Bob")
    assert(mutation.after(1) == 35, "age should be 35 — Avro resolution matches fields by name, not position")
  }

  // ============== Debezium Tests ==============

  private val debeziumEnvelopeSchemaStr =
    """{
      |  "type": "record", "name": "Envelope", "namespace": "debezium",
      |  "fields": [
      |    {"name": "op", "type": "string"},
      |    {"name": "before", "type": ["null", {
      |      "type": "record", "name": "Value",
      |      "fields": [
      |        {"name": "id",   "type": "int"},
      |        {"name": "name", "type": ["null", "string"], "default": null}
      |      ]
      |    }], "default": null},
      |    {"name": "after",  "type": ["null", "debezium.Value"], "default": null},
      |    {"name": "source", "type": {
      |      "type": "record", "name": "Source",
      |      "fields": [{"name": "ts_ms", "type": ["null", "long"], "default": null}]
      |    }},
      |    {"name": "ts_ms", "type": ["null", "long"], "default": null}
      |  ]
      |}""".stripMargin

  it should "use DebeziumAvroSerDe as delegate when debezium=true" in {
    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    freshClient.register("debezium-topic-value", new AvroSchema(debeziumEnvelopeSchemaStr))

    val topicInfo = TopicInfo(
      "debezium-topic",
      "kafka",
      Map(RegistryHostKey -> "localhost", DebeziumMutationMapper.DebeziumKey -> "true", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // Schema should include mutation columns
    val schema = serDe.schema
    assert(schema.fields.length == 4, s"Expected 4 fields (id, name, mutation_ts, is_before), got ${schema.fields.length}")
    assert(schema.fields(2).name == Constants.MutationTimeColumn)
    assert(schema.fields(2).fieldType == LongType)
    assert(schema.fields(3).name == Constants.ReversalColumn)
    assert(schema.fields(3).fieldType == BooleanType)
  }

  it should "deserialize Debezium UPDATE envelope into Mutation with both before and after when debezium=true" in {
    val ts = 1710631967915L
    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    freshClient.register("debezium-update-topic-value", new AvroSchema(debeziumEnvelopeSchemaStr))

    val topicInfo = TopicInfo(
      "debezium-update-topic",
      "kafka",
      Map(RegistryHostKey -> "localhost", DebeziumMutationMapper.DebeziumKey -> "true", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    val envelopeAvroSchema = AvroCodec.of(debeziumEnvelopeSchemaStr).schema
    val valueSchema = envelopeAvroSchema.getField("before").schema().getTypes.get(1)
    val sourceSchema = envelopeAvroSchema.getField("source").schema()

    def makeValue(id: Int, name: String) = {
      val r = new GenericData.Record(valueSchema)
      r.put("id", id)
      r.put("name", name)
      r
    }
    val source = new GenericData.Record(sourceSchema)
    source.put("ts_ms", ts: java.lang.Long)

    val codec = new AvroCodec(debeziumEnvelopeSchemaStr)
    val envelope = new GenericData.Record(envelopeAvroSchema)
    envelope.put("op", "u")
    envelope.put("before", makeValue(1, "Alice"))
    envelope.put("after", makeValue(1, "Alicia"))
    envelope.put("source", source)
    envelope.put("ts_ms", null)

    val mutation = serDe.fromBytes(codec.encodeBinary(envelope))

    // UPDATE: both before and after should be populated
    assert(mutation.before != null, "before should not be null for UPDATE")
    assert(mutation.after != null, "after should not be null for UPDATE")
    assert(mutation.before(1) == "Alice")
    assert(mutation.after(1) == "Alicia")
    assert(mutation.before(3) == (true: java.lang.Boolean), "before row should have is_before=true")
    assert(mutation.after(3) == (false: java.lang.Boolean), "after row should have is_before=false")
    assert(mutation.before(2) == (ts: java.lang.Long))
    assert(mutation.after(2) == (ts: java.lang.Long))
  }

  it should "throw IllegalArgumentException when debezium=true with a Protobuf schema" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message Envelope { string op = 1; }""".stripMargin
    val freshClient = new MockSchemaRegistryClient(Seq(protoSchemaProvider).asJava)
    freshClient.register("debezium-proto-value", new ProtobufSchema(proto3SchemaStr))

    val topicInfo = TopicInfo(
      "debezium-proto",
      "kafka",
      Map(RegistryHostKey -> "localhost", DebeziumMutationMapper.DebeziumKey -> "true", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)
    assertThrows[IllegalArgumentException] {
      serDe.schema
    }
  }
}
