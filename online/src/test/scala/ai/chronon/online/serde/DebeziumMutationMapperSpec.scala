package ai.chronon.online.serde

import ai.chronon.api.{BooleanType, Constants, IntType, LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DebeziumMutationMapperSpec extends AnyFlatSpec with Matchers {

  private val innerSchema = StructType("Value",
    Array(StructField("id", IntType), StructField("name", StringType)))

  private val outputSchema = DebeziumMutationMapper.buildOutputSchema(innerSchema)

  private val ts = 1710631967915L: java.lang.Long

  // Sample inner rows (without mutation columns)
  private val beforeInner: Array[Any] = Array(1: java.lang.Integer, "Alice")
  private val afterInner: Array[Any] = Array(1: java.lang.Integer, "Bob")

  "buildOutputSchema" should "append mutation_ts and is_before at the tail" in {
    outputSchema.fields.length shouldBe 4
    outputSchema.fields(2) shouldBe StructField(Constants.MutationTimeColumn, LongType)
    outputSchema.fields(3) shouldBe StructField(Constants.ReversalColumn, BooleanType)
  }

  it should "preserve inner schema name and fields" in {
    outputSchema.name shouldBe "Value"
    outputSchema.fields(0) shouldBe StructField("id", IntType)
    outputSchema.fields(1) shouldBe StructField("name", StringType)
  }

  "appendMutationColumns" should "produce an array with mutation_ts and is_before appended" in {
    val result = DebeziumMutationMapper.appendMutationColumns(beforeInner, ts, isBefore = true)
    result.length shouldBe 4
    result(0) shouldBe (1: java.lang.Integer)
    result(1) shouldBe "Alice"
    result(2) shouldBe ts
    result(3) shouldBe (true: java.lang.Boolean)
  }

  it should "set is_before=false when isBefore is false" in {
    val result = DebeziumMutationMapper.appendMutationColumns(afterInner, ts, isBefore = false)
    result(3) shouldBe (false: java.lang.Boolean)
  }

  it should "handle null mutationTs" in {
    val result = DebeziumMutationMapper.appendMutationColumns(afterInner, null, isBefore = false)
    result(2) shouldBe (null: AnyRef)
  }

  "toMutation (op=c)" should "produce before=null, after populated, is_before=false" in {
    val m = DebeziumMutationMapper.toMutation("c", null, afterInner, ts, outputSchema)
    m.before shouldBe null
    m.after should not be null
    m.after(3) shouldBe (false: java.lang.Boolean)
    m.after(2) shouldBe ts
  }

  "toMutation (op=r)" should "behave identically to op=c (snapshot read)" in {
    val m = DebeziumMutationMapper.toMutation("r", null, afterInner, ts, outputSchema)
    m.before shouldBe null
    m.after should not be null
    m.after(3) shouldBe (false: java.lang.Boolean)
  }

  "toMutation (op=d)" should "produce before populated with is_before=true, after=null" in {
    val m = DebeziumMutationMapper.toMutation("d", beforeInner, null, ts, outputSchema)
    m.after shouldBe null
    m.before should not be null
    m.before(3) shouldBe (true: java.lang.Boolean)
    m.before(2) shouldBe ts
  }

  "toMutation (op=u)" should "populate both before (is_before=true) and after (is_before=false)" in {
    val m = DebeziumMutationMapper.toMutation("u", beforeInner, afterInner, ts, outputSchema)
    m.before should not be null
    m.after should not be null
    m.before(3) shouldBe (true: java.lang.Boolean)
    m.after(3) shouldBe (false: java.lang.Boolean)
    m.before(1) shouldBe "Alice"
    m.after(1) shouldBe "Bob"
  }

  it should "set mutation_ts on both before and after rows" in {
    val m = DebeziumMutationMapper.toMutation("u", beforeInner, afterInner, ts, outputSchema)
    m.before(2) shouldBe ts
    m.after(2) shouldBe ts
  }

  "toMutation with malformed data" should "return a no-op Mutation for op=c with null after" in {
    val m = DebeziumMutationMapper.toMutation("c", null, null, ts, outputSchema)
    m.before shouldBe null
    m.after shouldBe null
  }

  it should "return a no-op Mutation for op=d with null before" in {
    val m = DebeziumMutationMapper.toMutation("d", null, null, ts, outputSchema)
    m.before shouldBe null
    m.after shouldBe null
  }

  it should "return a no-op Mutation for op=u with null before" in {
    val m = DebeziumMutationMapper.toMutation("u", null, afterInner, ts, outputSchema)
    m.before shouldBe null
    m.after shouldBe null
  }

  it should "return a no-op Mutation for op=u with null after" in {
    val m = DebeziumMutationMapper.toMutation("u", beforeInner, null, ts, outputSchema)
    m.before shouldBe null
    m.after shouldBe null
  }

  "toMutation with unrecognised op" should "return a no-op Mutation for op=t (truncate)" in {
    val m = DebeziumMutationMapper.toMutation("t", null, null, ts, outputSchema)
    m.before shouldBe null
    m.after shouldBe null
  }

  it should "return a no-op Mutation for any other unknown op" in {
    val m = DebeziumMutationMapper.toMutation("x", null, afterInner, ts, outputSchema)
    m.before shouldBe null
    m.after shouldBe null
  }
}
