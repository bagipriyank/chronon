package ai.chronon.flink.test.window

import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.api._
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.window.FlinkRowAggregationFunction
import ai.chronon.online.TileCodec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.util.Failure
import scala.util.Try
import ai.chronon.flink.test.FlinkTestUtils

class FlinkRowAggregationFunctionTest extends AnyFlatSpec {
  private val aggregations: Seq[Aggregation] = Seq(
    Builders.Aggregation(
      Operation.AVERAGE,
      "views",
      Seq(
        new Window(1, TimeUnit.DAYS),
        new Window(1, TimeUnit.HOURS),
        new Window(30, TimeUnit.DAYS)
      )
    ),
    Builders.Aggregation(
      Operation.AVERAGE,
      "rating",
      Seq(
        new Window(1, TimeUnit.DAYS),
        new Window(1, TimeUnit.HOURS)
      )
    ),
    Builders.Aggregation(
      Operation.MAX,
      "title",
      Seq(
        new Window(1, TimeUnit.DAYS)
      )
    ),
    Builders.Aggregation(
      Operation.LAST,
      "title",
      Seq(
        new Window(1, TimeUnit.DAYS)
      )
    ),
    Builders.Aggregation(
      Operation.UNIQUE_TOP_K,
      "struct_col",
      Seq(
        new Window(1, TimeUnit.DAYS)
      ),
      argMap = Map("k" -> "2")
    )
  )

  private val schema = List(
    Constants.TimeColumn -> LongType,
    "views" -> IntType,
    "rating" -> FloatType,
    "title" -> StringType,
    "struct_col" -> StructType(
      "struct_col",
      Array(
        StructField("unique_id", LongType),
        StructField("sort_key", StringType),
        StructField("payload", StringType)
      )
    )
  )

  it should "flink aggregator produces correct results" in {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val aggregateFunc = new FlinkRowAggregationFunction(groupBy, schema)

    var acc = aggregateFunc.createAccumulator()
    val rows = Seq(
      createRow(1519862399984L, 4, 4.0f, "A", Struct(1, "8", "a")),
      createRow(1519862399984L, 40, 5.0f, "B", Struct(1, "8", "a")),
      createRow(1519862399988L, 3, 3.0f, "C", Struct(2, "7", "b")),
      createRow(1519862399988L, 5, 4.0f, "D", Struct(2, "7", "b")),
      createRow(1519862399994L, 4, 4.0f, "A", Struct(3, "6", "c")),
      createRow(1519862399999L, 10, 4.0f, "A", Struct(4, "5", "d"))
    )
    rows.foreach(row => acc = aggregateFunc.add(row, acc))
    val result = aggregateFunc.getResult(acc)

    // we sanity check the final result of the accumulator
    // to do so, we must first expand / decompress the windowed tile IR into a full tile
    // then we can finalize the tile and get the final result
    val tileCodec = new TileCodec(groupBy, schema)
    val expandedIr = tileCodec.expandWindowedTileIr(result.ir)
    val finalResult = tileCodec.windowedRowAggregator.finalize(expandedIr)

    assert(finalResult.length == 8)
    val expectedAvgViews = 11.0f
    val expectedAvgRating = 4.0f
    val expectedMax = "D"
    val expectedLast = "A"
    val expectedUniqueTopK = Seq(
      Map(
        "unique_id" -> 1,
        "sort_key" -> "8",
        "payload" -> "a"
      ),
      Map(
        "unique_id" -> 2,
        "sort_key" -> "7",
        "payload" -> "b"
      )
    ).toJava

    val expectedResult = Array(
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgRating,
      expectedAvgRating,
      expectedMax,
      expectedLast,
      expectedUniqueTopK
    )

    finalResult.zip(expectedResult).foreach { case (computed, expected) =>
      computed shouldBe expected
    }
  }

  it should "flink aggregator results can be merged with other pre aggregates" in {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val aggregateFunc = new FlinkRowAggregationFunction(groupBy, schema)

    // create partial aggregate 1
    var acc1 = aggregateFunc.createAccumulator()
    val rows1 = Seq(
      createRow(1519862399984L, 4, 4.0f, "A", Struct(3, "9", "a")),
      createRow(1519862399984L, 40, 5.0f, "B", Struct(3, "9", "a"))
    )
    rows1.foreach(row => acc1 = aggregateFunc.add(row, acc1))
    val partialResult1 = aggregateFunc.getResult(acc1)

    // create partial aggregate 2
    var acc2 = aggregateFunc.createAccumulator()
    val rows2 = Seq(
      createRow(1519862399988L, 3, 3.0f, "C", Struct(1, "8", "a")),
      createRow(1519862399988L, 5, 4.0f, "D", Struct(2, "7", "b"))
    )
    rows2.foreach(row => acc2 = aggregateFunc.add(row, acc2))
    val partialResult2 = aggregateFunc.getResult(acc2)

    // create partial aggregate 3
    var acc3 = aggregateFunc.createAccumulator()
    val rows3 = Seq(
      createRow(1519862399994L, 4, 4.0f, "A", Struct(4, "5", "c")),
      createRow(1519862399999L, 10, 4.0f, "A", Struct(5, "6", "d"))
    )
    rows3.foreach(row => acc3 = aggregateFunc.add(row, acc3))
    val partialResult3 = aggregateFunc.getResult(acc3)

    // lets merge the partial results together and check
    val mergedPartialAggregates = aggregateFunc.rowAggregator
      .merge(
        aggregateFunc.rowAggregator.merge(partialResult1.ir, partialResult2.ir),
        partialResult3.ir
      )

    // we sanity check the final result of the accumulator
    // to do so, we must first expand / decompress the windowed tile IR into a full tile
    // then we can finalize the tile and get the final result
    val tileCodec = new TileCodec(groupBy, schema)
    val expandedIr = tileCodec.expandWindowedTileIr(mergedPartialAggregates)
    val finalResult = tileCodec.windowedRowAggregator.finalize(expandedIr)

    assert(finalResult.length == 8)
    val expectedAvgViews = 11.0f
    val expectedAvgRating = 4.0f
    val expectedMax = "D"
    val expectedLast = "A"
    val expectedUniqueTopK = Seq(
      Map(
        "unique_id" -> 3,
        "sort_key" -> "9",
        "payload" -> "a"
      ),
      Map(
        "unique_id" -> 1,
        "sort_key" -> "8",
        "payload" -> "a"
      )
    ).toJava
    val expectedResult = Array(
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgRating,
      expectedAvgRating,
      expectedMax,
      expectedLast,
      expectedUniqueTopK
    )
    finalResult.zip(expectedResult).foreach { case (computed, expected) =>
      computed shouldBe expected
    }
  }

  it should "flink aggregator produces correct results if input is in incorrect order" in {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val aggregateFunc = new FlinkRowAggregationFunction(groupBy, schema)

    var acc = aggregateFunc.createAccumulator()

    // Create a map where the entries are not in the same order as `schema`.
    val outOfOrderRow = Map[String, Any](
      "rating" -> 4.0f,
      Constants.TimeColumn -> 1519862399999L,
      "title" -> "A",
      "views" -> 10,
      "struct_col" -> Map(
        "unique_id" -> 1L,
        "sort_key" -> "8",
        "payload" -> "a"
      )
    )
    val outOfOrderRowEvent = ProjectedEvent(outOfOrderRow, 123L)

    // If the aggregator fails to fix the order, we'll get a ClassCastException
    Try {
      acc = aggregateFunc.add(outOfOrderRowEvent, acc)
    } match {
      case Failure(e) => {
        e.printStackTrace()
        fail(
          "An exception was thrown by the aggregator when it should not have been. " +
            s"The aggregator should fix the order without failing. $e")
      }
      case _ =>
    }

    val result = aggregateFunc.getResult(acc)

    // we sanity check the final result of the accumulator
    // to do so, we must first expand / decompress the windowed tile IR into a full tile
    // then we can finalize the tile and get the final result
    val tileCodec = new TileCodec(groupBy, schema)
    val expandedIr = tileCodec.expandWindowedTileIr(result.ir)
    val finalResult = tileCodec.windowedRowAggregator.finalize(expandedIr)
    assert(finalResult.length == 8)

    val expectedResult = Array(
      outOfOrderRow("views"),
      outOfOrderRow("views"),
      outOfOrderRow("views"),
      outOfOrderRow("rating"),
      outOfOrderRow("rating"),
      outOfOrderRow("title"),
      outOfOrderRow("title"),
      Seq(outOfOrderRow("struct_col")).toJava
    )
    finalResult.zip(expectedResult).foreach { case (computed, expected) =>
      computed shouldBe expected
    }
  }

  // ---- Mutation (CDC / EntitySource) tests ----

  // Schema for entity mutation tests: matches makeEntityGroupBy — includes is_before and mutation_ts
  private val mutationSchema = List(
    Constants.TimeColumn       -> LongType,
    "double_val"               -> DoubleType,
    Constants.MutationTimeColumn -> LongType,
    Constants.ReversalColumn   -> BooleanType
  )

  private def makeMutationGroupBy = FlinkTestUtils.makeEntityGroupBy(Seq("id"))

  private def mutationRow(ts: Long, doubleVal: Double, mutationTs: Long, isBefore: Boolean): ProjectedEvent =
    ProjectedEvent(
      Map(
        Constants.TimeColumn       -> ts,
        "double_val"               -> doubleVal,
        Constants.MutationTimeColumn -> mutationTs,
        Constants.ReversalColumn   -> isBefore
      ),
      0L
    )

  private def finalizeIr(groupBy: GroupBy, ir: Array[Any]): Array[Any] = {
    val tileCodec = new TileCodec(groupBy, mutationSchema)
    tileCodec.windowedRowAggregator.finalize(tileCodec.expandWindowedTileIr(ir))
  }

  it should "insert (isBefore=false) adds value to IR" in {
    val groupBy = makeMutationGroupBy
    val agg = new FlinkRowAggregationFunction(groupBy, mutationSchema)
    var acc = agg.createAccumulator()
    acc = agg.add(mutationRow(1L, 10.0, 2L, isBefore = false), acc)

    val result = finalizeIr(groupBy, agg.getResult(acc).ir)
    result(0).asInstanceOf[Double] shouldBe 10.0
  }

  it should "delete (isBefore=true) subtracts value from IR" in {
    val groupBy = makeMutationGroupBy
    val agg = new FlinkRowAggregationFunction(groupBy, mutationSchema)
    var acc = agg.createAccumulator()
    // Create then delete the same value
    acc = agg.add(mutationRow(1L, 10.0, 2L, isBefore = false), acc)
    acc = agg.add(mutationRow(1L, 10.0, 3L, isBefore = true), acc)

    val result = finalizeIr(groupBy, agg.getResult(acc).ir)
    result(0).asInstanceOf[Double] shouldBe 0.0
  }

  it should "update (before then after) produces net after value" in {
    val groupBy = makeMutationGroupBy
    val agg = new FlinkRowAggregationFunction(groupBy, mutationSchema)
    var acc = agg.createAccumulator()
    // Initial create
    acc = agg.add(mutationRow(1L, 10.0, 2L, isBefore = false), acc)
    // Update: subtract old (before), add new (after)
    acc = agg.add(mutationRow(1L, 10.0, 3L, isBefore = true), acc)
    acc = agg.add(mutationRow(1L, 25.0, 3L, isBefore = false), acc)

    val result = finalizeIr(groupBy, agg.getResult(acc).ir)
    result(0).asInstanceOf[Double] shouldBe 25.0
  }

  it should "multiple updates produce correct net IR" in {
    val groupBy = makeMutationGroupBy
    val agg = new FlinkRowAggregationFunction(groupBy, mutationSchema)
    var acc = agg.createAccumulator()
    acc = agg.add(mutationRow(1L, 5.0,  2L, isBefore = false), acc) // create +5
    acc = agg.add(mutationRow(1L, 5.0,  3L, isBefore = true),  acc) // update before -5
    acc = agg.add(mutationRow(1L, 15.0, 3L, isBefore = false), acc) // update after +15
    acc = agg.add(mutationRow(1L, 15.0, 4L, isBefore = true),  acc) // update before -15
    acc = agg.add(mutationRow(1L, 30.0, 4L, isBefore = false), acc) // update after +30

    val result = finalizeIr(groupBy, agg.getResult(acc).ir)
    result(0).asInstanceOf[Double] shouldBe 30.0
  }

  it should "throw at construction when is_before column is absent for mutation source" in {
    val groupBy = makeMutationGroupBy
    // Schema missing ReversalColumn — should fail fast at construction
    val schemaWithoutReversal = List(
      Constants.TimeColumn         -> LongType,
      "double_val"                 -> DoubleType,
      Constants.MutationTimeColumn -> LongType
      // ReversalColumn intentionally omitted
    )
    val ex = intercept[IllegalArgumentException] {
      new FlinkRowAggregationFunction(groupBy, schemaWithoutReversal)
    }
    assert(ex.getMessage.contains("reversal_column"))
  }

  it should "not treat event source as mutation even if schema has is_before" in {
    // Event-source GroupBy (no mutationTopic) → isMutation=false → isBefore column ignored
    val eventGroupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val schemaWithReversal = List(
      Constants.TimeColumn     -> LongType,
      "double_val"             -> DoubleType,
      Constants.ReversalColumn -> BooleanType
    )
    val agg = new FlinkRowAggregationFunction(eventGroupBy, schemaWithReversal)
    var acc = agg.createAccumulator()
    // Add a row with isBefore=true — for event source, this should be treated as an insert
    acc = agg.add(
      ProjectedEvent(
        Map(
          Constants.TimeColumn     -> 1L,
          "double_val"             -> 10.0,
          Constants.ReversalColumn -> true
        ),
        0L
      ),
      acc
    )
    val result = finalizeIr(eventGroupBy, agg.getResult(acc).ir)
    // For event sources, isBefore is ignored — value should be added, not subtracted
    result(0).asInstanceOf[Double] shouldBe 10.0
  }

  case class Struct(uniqueId: Long, sortKey: String, payload: String)
  def createRow(ts: Long, views: Int, rating: Float, title: String, struct: Struct): ProjectedEvent = {
    val row = Map(
      Constants.TimeColumn -> ts,
      "views" -> views,
      "rating" -> rating,
      "title" -> title,
      "struct_col" -> Map(
        "unique_id" -> struct.uniqueId,
        "sort_key" -> struct.sortKey,
        "payload" -> struct.payload
      )
    )
    ProjectedEvent(row, 123L)
  }
}
