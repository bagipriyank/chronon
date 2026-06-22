/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.{FinalBatchIr, FiveMinuteResolution, SawtoothOnlineAggregator, TiledIr}
import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api._
import com.google.gson.Gson
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import scala.collection.mutable

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.Locale

class SawtoothOnlineAggregatorTest extends AnyFlatSpec {


  it should "ensure consistency between sawtooth ir and online ir" in {
    val queryEndTs = TsUtils.round(System.currentTimeMillis(), WindowUtils.Day.millis)
    val batchEndTs = queryEndTs - WindowUtils.Day.millis
    val queries = CStream.genTimestamps(new Window(1, TimeUnit.DAYS), 1000)
    val eventCount = 10000

    val columns = Seq(Column("ts", LongType, 60),
                      Column("num", LongType, 100),
                      Column("user", StringType, 6000),
                      Column("ts_col", StringType, 60))
    val formatter = DateTimeFormatter
      .ofPattern("MM-dd HH:mm:ss", Locale.US)
      .withZone(ZoneOffset.UTC)
    val RowsWithSchema(events, schema) = CStream.gen(columns, eventCount)
    events.foreach { row => row.set(3, formatter.format(Instant.ofEpochMilli(row.get(0).asInstanceOf[Long]))) }

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        operation = Operation.COUNT,
        inputColumn = "num",
        windows = Seq(new Window(14, TimeUnit.DAYS),
                      new Window(20, TimeUnit.HOURS),
                      new Window(6, TimeUnit.DAYS),
                      new Window(7, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "num",
        windows = Seq(new Window(14, TimeUnit.DAYS),
                      new Window(20, TimeUnit.HOURS),
                      new Window(6, TimeUnit.DAYS),
                      new Window(7, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.FIRST,
        inputColumn = "ts_col",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.LAST,
        inputColumn = "ts_col",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.SUM,
        inputColumn = "num",
        windows = null
      ),
      Builders.Aggregation(
        operation = Operation.UNIQUE_COUNT,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.APPROX_UNIQUE_COUNT,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.LAST_K,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.FIRST_K,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.TOP_K,
        inputColumn = "num",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.MIN,
        inputColumn = "num",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.MAX,
        inputColumn = "num",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      )
    )

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
    val onlineAggregator = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, FiveMinuteResolution)
    val (events1, events2) = events.splitAt(eventCount / 2)
    val batchIr1 = events1.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr2 = events2.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr = onlineAggregator.normalizeBatchIr(onlineAggregator.merge(batchIr1, batchIr2))
    val denormBatchIr = onlineAggregator.denormalizeBatchIr(batchIr)
    val windowHeadEvents = events.filter(_.ts >= batchEndTs)
    val onlineIrs = queries.map(onlineAggregator.lambdaAggregateIr(denormBatchIr, windowHeadEvents.iterator, _))

    val gson = new Gson()
    for (i <- queries.indices) {
      val onlineStr = gson.toJson(onlineAggregator.windowedAggregator.finalize(onlineIrs(i)))
      val sawtoothStr = gson.toJson(onlineAggregator.windowedAggregator.finalize(sawtoothIrs(i)))
      assertEquals(sawtoothStr, onlineStr)
    }
  }

  // Minimal Row impl for event rows (no mutation fields needed)
  private class SimpleRow(value: Long, tsMillis: Long) extends Row {
    override def get(index: Int): Any = if (index == 0) value else throw new IndexOutOfBoundsException(index.toString)
    override val length: Int = 1
    override def ts: Long = tsMillis
    override def isBefore: Boolean = false
    override def mutationTs: Long = tsMillis
  }

  // Row impl for mutation rows — isBefore and mutationTs are set per-row
  private class MutationRow(value: Long, tsMillis: Long, mutationTsMillis: Long, val isBefore: Boolean) extends Row {
    override def get(index: Int): Any = if (index == 0) value else throw new IndexOutOfBoundsException(index.toString)
    override val length: Int = 1
    override def ts: Long = tsMillis
    override def mutationTs: Long = mutationTsMillis
  }

  // batchEndTs sits 1 day before queryTs so streaming events land in the correct window
  private val batchEndTs  = TsUtils.round(System.currentTimeMillis(), WindowUtils.Day.millis) - WindowUtils.Day.millis
  private val queryTs     = batchEndTs + WindowUtils.Day.millis
  // A timestamp that falls in the streaming window (after batchEndTs, before queryTs)
  private val streamingTs = batchEndTs + WindowUtils.Hour.millis

  private val entityAggregations: Seq[Aggregation] = Seq(
    Builders.Aggregation(Operation.SUM,   inputColumn = "value", windows = Seq(new Window(2, TimeUnit.DAYS))),
    Builders.Aggregation(Operation.COUNT, inputColumn = "value", windows = Seq(new Window(2, TimeUnit.DAYS)))
  )

  private val entitySchema: Seq[(String, DataType)] = Seq("value" -> LongType)

  /** Simulates the streaming write side: runs mutation rows through the windowed RowAggregator with
    * delete/update semantics and returns a TiledIr.
    */
  private def buildTiledIr(aggregator: SawtoothOnlineAggregator,
                            rows: Seq[(Row, Boolean)],
                            ts: Long): TiledIr = {
    val windowedAgg = aggregator.windowedAggregator
    val ir = windowedAgg.init
    rows.foreach { case (row, isBefore) =>
      if (isBefore) windowedAgg.delete(ir, row) else windowedAgg.update(ir, row)
    }
    TiledIr(ts, ir)
  }

  private def buildBatchIr(aggregator: SawtoothOnlineAggregator, rows: Seq[Row]): FinalBatchIr = {
    aggregator.normalizeBatchIr(rows.foldLeft(aggregator.init)(aggregator.update))
  }

  it should "lambdaAggregateFinalizedTiled matches lambdaAggregateFinalized for pure inserts" in {
    // Insert-only: tiled and non-tiled paths must produce identical results.
    val aggregator = new SawtoothOnlineAggregator(batchEndTs, entityAggregations, entitySchema, FiveMinuteResolution)
    val batchIr    = buildBatchIr(aggregator, Seq(new SimpleRow(10L, batchEndTs - 1000)))

    val insert1 = new SimpleRow(3L, streamingTs)
    val insert2 = new SimpleRow(5L, streamingTs + 1000)

    val nonTiledResult = aggregator.lambdaAggregateFinalized(
      batchIr, Seq(insert1, insert2).iterator, queryTs, hasReversal = false)

    val tileIr      = buildTiledIr(aggregator, Seq(insert1 -> false, insert2 -> false), streamingTs)
    val tiledResult = aggregator.lambdaAggregateFinalizedTiled(batchIr, Iterator(tileIr), queryTs)

    tiledResult.length shouldBe nonTiledResult.length
    tiledResult.zip(nonTiledResult).foreach { case (a, b) => a shouldBe b }
  }

  it should "lambdaAggregateFinalizedTiled produces correct net result after insert + delete + reinsert" in {
    // Mutation sequence within one tile: insert 7, delete 7 (isBefore=true), insert 4.
    // Net tile: sum=4, count=1. The tiled path pre-applies these semantics; the non-tiled path
    // processes raw mutation rows with hasReversal=true. Both must agree.
    val aggregator = new SawtoothOnlineAggregator(batchEndTs, entityAggregations, entitySchema, FiveMinuteResolution)
    val batchIr    = buildBatchIr(aggregator, Seq(new SimpleRow(10L, batchEndTs - 1000)))

    // Non-tiled path: raw mutation rows processed by lambdaAggregateFinalized with hasReversal=true
    val mutRow1 = new MutationRow(7L, streamingTs,        streamingTs,        isBefore = false)
    val mutRow2 = new MutationRow(7L, streamingTs,        streamingTs,        isBefore = true)
    val mutRow3 = new MutationRow(4L, streamingTs + 1000, streamingTs + 1000, isBefore = false)
    val nonTiledResult = aggregator.lambdaAggregateFinalized(
      batchIr, Seq(mutRow1, mutRow2, mutRow3).iterator, queryTs, hasReversal = true)

    // Tiled path: tile IR is the net result after Flink job's applied delete/update semantics
    val rawInsert1 = new SimpleRow(7L, streamingTs)
    val rawDelete1 = new SimpleRow(7L, streamingTs)
    val rawInsert2 = new SimpleRow(4L, streamingTs + 1000)
    val tileIr     = buildTiledIr(aggregator,
                                  Seq(rawInsert1 -> false, rawDelete1 -> true, rawInsert2 -> false),
                                  streamingTs)
    val tiledResult = aggregator.lambdaAggregateFinalizedTiled(batchIr, Iterator(tileIr), queryTs)

    tiledResult.length shouldBe nonTiledResult.length
    tiledResult.zip(nonTiledResult).foreach { case (a, b) => a shouldBe b }
  }

  it should "lambdaAggregateFinalizedTiled with null streaming returns same as non-tiled batch-only" in {
    val aggregator = new SawtoothOnlineAggregator(batchEndTs, entityAggregations, entitySchema, FiveMinuteResolution)
    val batchIr    = buildBatchIr(aggregator, Seq(new SimpleRow(10L, batchEndTs - 1000)))

    val tiledResult    = aggregator.lambdaAggregateFinalizedTiled(batchIr, null, queryTs)
    val nonTiledResult = aggregator.lambdaAggregateFinalized(batchIr, null, queryTs)

    tiledResult.length shouldBe nonTiledResult.length
    tiledResult.zip(nonTiledResult).foreach { case (a, b) => a shouldBe b }
  }

  it should "test updateNullCounts when collapsedIr is null and tailHops is not null" in {
    val endPartition = "2023-08-20"
    val endTs = PartitionSpec.daily.epochMillis(endPartition)

    val numDays = 18
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "num",
        windows = Seq(
          new Window(numDays, TimeUnit.DAYS))
      )
    )
    val onlineAggregator = new SawtoothOnlineAggregator(endTs, aggregations, Seq(
      ("ts", LongType),
      ("num", LongType),
      ("user", StringType),
      ("ts_col", StringType)
    ), FiveMinuteResolution)


    // subtract numDays from endTs to get the start of the window
    val windowStartTs = endTs - WindowUtils.Day.millis * numDays

    // add 1 day to get a timestamp that is within the window but after the batch end ts
    val eligibleTailHopTs = windowStartTs + WindowUtils.Day.millis * 1

    val sampleBatchIr = FinalBatchIr(
      collapsed = Array.fill(onlineAggregator.windowedAggregator.length)(null),
      tailHops = Array(
        Array(Array(Array(1.0, 1), eligibleTailHopTs)),
        Array(),
        Array()
      )
    )

    val nullCountMap = mutable.HashMap.empty[String, Long]

    onlineAggregator.updateNullCounts(batchIr = sampleBatchIr, nullCounts = nullCountMap)
    nullCountMap.size shouldBe 0 // collapsed is null but tailHops has non-null value
  }

  it should "test updateNullCounts when collapsedIr is not null and tailHops is null" in {
    val endPartition = "2023-08-20"
    val endTs = PartitionSpec.daily.epochMillis(endPartition)

    val queryEndTs = TsUtils.round(endTs, WindowUtils.Day.millis)
    val batchEndTs = queryEndTs - WindowUtils.Day.millis

    val numDays = 18
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "num",
        windows = Seq(
          new Window(numDays, TimeUnit.DAYS))
      )
    )
    val onlineAggregator = new SawtoothOnlineAggregator(batchEndTs, aggregations, Seq(
      ("ts", LongType),
      ("num", LongType),
      ("user", StringType),
      ("ts_col", StringType)
    ), FiveMinuteResolution)

    // subtract numDays from endTs to get the start of the window
    val windowStartTs = endTs - WindowUtils.Day.millis * numDays

    // add two days to get a timestamp that is within the window but after the batch end ts
    val eligibleTailHopTs = windowStartTs + WindowUtils.Day.millis * 2

    val sampleBatchIr = FinalBatchIr(
      collapsed = Array(Array((461.0, 87L))),
      tailHops = Array(
        Array(Array(null, eligibleTailHopTs)),
        Array(),
        Array()
      )
    )

    val nullCountMap = mutable.HashMap.empty[String, Long]

    onlineAggregator.updateNullCounts(batchIr = sampleBatchIr, nullCounts = nullCountMap)
    nullCountMap.size shouldBe 0 // collapsed is not null
  }

  it should "test updateNullCounts when collapsedIr is null and tailHops is null" in {
    val endPartition = "2023-08-20"
    val endTs = PartitionSpec.daily.epochMillis(endPartition)

    val queryEndTs = TsUtils.round(endTs, WindowUtils.Day.millis)
    val batchEndTs = queryEndTs - WindowUtils.Day.millis

    val numDays = 18
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "num",
        windows = Seq(
          new Window(numDays, TimeUnit.DAYS))
      )
    )
    val onlineAggregator = new SawtoothOnlineAggregator(batchEndTs, aggregations, Seq(
      ("ts", LongType),
      ("num", LongType),
      ("user", StringType),
      ("ts_col", StringType)
    ), FiveMinuteResolution)


    // subtract numDays from endTs to get the start of the window
    val windowStartTs = endTs - WindowUtils.Day.millis * numDays

    // add two days to get a timestamp that is within the window but after the batch end ts
    val eligibleTailHopTs = windowStartTs + WindowUtils.Day.millis * 2

    val sampleBatchIr = FinalBatchIr(
      collapsed = Array.fill(onlineAggregator.windowedAggregator.length)(null),
      tailHops = Array(
        Array(Array(null, eligibleTailHopTs)),
        Array(),
        Array()
      )
    )

    val nullCountMap = mutable.HashMap.empty[String, Long]

    onlineAggregator.updateNullCounts(batchIr = sampleBatchIr, nullCounts = nullCountMap)
    nullCountMap.size shouldBe 1
  }

  it should "test updateNullCounts when collapsedIr is not null and tailHops is not null" in {
    val endPartition = "2023-08-20"
    val endTs = PartitionSpec.daily.epochMillis(endPartition)

    val queryEndTs = TsUtils.round(endTs, WindowUtils.Day.millis)
    val batchEndTs = queryEndTs - WindowUtils.Day.millis

    val numDays = 18
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "num",
        windows = Seq(
          new Window(numDays, TimeUnit.DAYS))
      )
    )
    val onlineAggregator = new SawtoothOnlineAggregator(batchEndTs, aggregations, Seq(
      ("ts", LongType),
      ("num", LongType),
      ("user", StringType),
      ("ts_col", StringType)
    ), FiveMinuteResolution)


    // subtract numDays from endTs to get the start of the window
    val windowStartTs = endTs - WindowUtils.Day.millis * numDays

    // add two days to get a timestamp that is within the window but after the batch end ts
    val eligibleTailHopTs = windowStartTs + WindowUtils.Day.millis * 2

    val sampleBatchIr = FinalBatchIr(
      collapsed = Array(Array((461.0, 87L))),
      tailHops = Array(
        Array(Array(Array(1.0, 1), eligibleTailHopTs)),
        Array(),
        Array()
      )
    )

    val nullCountMap = mutable.HashMap.empty[String, Long]

    onlineAggregator.updateNullCounts(batchIr = sampleBatchIr, nullCounts = nullCountMap)
    nullCountMap.size shouldBe 0
  }
}
