package ai.chronon.flink.test

import ai.chronon.api.Constants.{ReversalColumn, TimeColumn}
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{GroupBy, TilingUtils}
import ai.chronon.flink.{FlinkGroupByStreamingJob, SparkExpressionEval, SparkExpressionEvalFn}
import ai.chronon.flink.types.{TimestampedIR, TimestampedTile, WriteResponse}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.{Api, GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.mockito.Mockito.withSettings
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock



// Flink Job Integration Test for Entity-based GroupBys
class FlinkJobEntityIntegrationTest extends AnyFlatSpec with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build)

  before {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
  }

  it should "flink job end to end" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // mutation timestamps are 1000ms (1s) after the created timestamps
    val elements = Seq(
      E2ETestMutationEvent("test1", 12, 1.5, 1699366993123L, 1699366993123L + 1000, isBefore = true),
      E2ETestMutationEvent("test2", 13, 1.6, 1699366993124L, 1699366993124L + 1000, isBefore = false),
      E2ETestMutationEvent("test3", 14, 1.7, 1699366993125L, 1699366993125L + 1000, isBefore = true)
    )

    val groupBy = FlinkTestUtils.makeEntityGroupBy(Seq("id"))
    val (job, groupByServingInfoParsed) = buildFlinkJob(groupBy, elements)
    job.runGroupByJob(env).addSink(new CollectSink)

    env.execute("FlinkJobEntityIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.toScala

    writeEventCreatedDS.size shouldBe elements.size

    // check that the timestamps of the written out events match the input events - timestamp on the write requests matches
    // mutation time
    writeEventCreatedDS.map(_.tsMillis).toSet shouldBe elements.map(_.mutationTime).toSet
    // check that all the writes were successful
    writeEventCreatedDS.map(_.status) shouldBe Seq(true, true, true)

    // let's crack open the written value bytes and check that the values are correct
    val valueGenRecords =
      writeEventCreatedDS.map(in => groupByServingInfoParsed.mutationValueAvroCodec.decode(in.valueBytes))
    val mutationFieldsSet = valueGenRecords
      .map(r => (r.get(TimeColumn).asInstanceOf[Long], r.get(ReversalColumn).asInstanceOf[Boolean]))
      .toSet
    val expectedMutationFieldsSet = elements.map(e => (e.created, e.isBefore)).toSet

    mutationFieldsSet shouldBe expectedMutationFieldsSet
  }

  // Tile decode helpers — mirrors FlinkJobEventIntegrationTest

  private def avroConvertPutRequestToTimestampedTile(in: WriteResponse,
                                                     gbInfo: GroupByServingInfoParsed): TimestampedTile = {
    val tileKey  = TilingUtils.deserializeTileKey(in.keyBytes)
    val keyBytes = tileKey.keyBytes.toScala.toArray.map(_.asInstanceOf[Byte])
    val record   = gbInfo.keyCodec.decode(keyBytes)
    val decodedKeys: List[String] =
      gbInfo.groupBy.keyColumns.toScala.map(record.get(_).toString)
    new TimestampedTile(decodedKeys.map(_.asInstanceOf[Any]).toJava, in.valueBytes, in.tsMillis, in.startProcessingTime)
  }

  private def avroConvertTimestampedTileToTimestampedIR(tile: TimestampedTile,
                                                        gbInfo: GroupByServingInfoParsed): TimestampedIR = {
    val tileIR = gbInfo.tiledCodec.decodeTileIr(tile.tileBytes)
    new TimestampedIR(tileIR._1, Some(tile.latestTsMillis), Some(tile.startProcessingTime), None)
  }

  // Returns the last-emitted IR per key from a collection of WriteResponses.
  private def finalIRsPerKey(
      responses: Seq[WriteResponse],
      gbInfo: GroupByServingInfoParsed
  ): Map[Seq[Any], List[Any]] =
    responses
      .map { wr =>
        val tile = avroConvertPutRequestToTimestampedTile(wr, gbInfo)
        val ir   = avroConvertTimestampedTileToTimestampedIR(tile, gbInfo)
        (tile.keys, ir.ir.toList, wr.tsMillis)
      }
      .groupBy(_._1)
      .map { case (keys, triples) => (keys.toScala, triples.maxBy(_._3)._2) }

  // ---- Tiled entity integration tests ----

  it should "tiled entity job — create events produce correct IRs" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val elements = Seq(
      E2ETestMutationEvent("id1", 1, 10.0, 1L, 2L, isBefore = false),
      E2ETestMutationEvent("id2", 1, 20.0, 3L, 4L, isBefore = false),
      E2ETestMutationEvent("id3", 1, 30.0, 5L, 6L, isBefore = false)
    )

    val groupBy = FlinkTestUtils.makeEntityGroupBy(Seq("id"))
    val (job, gbInfo) = buildFlinkJob(groupBy, elements)
    job.runTiledGroupByJob(env).addSink(new CollectSink)
    env.execute("TiledEntityJobCreates")

    val responses = CollectSink.values.toScala
    responses.forall(_.status) shouldBe true

    val irsByKey = finalIRsPerKey(responses, gbInfo)
    // Each key has one event → sum equals the double_val
    irsByKey(List("id1"))(0).asInstanceOf[Double] shouldBe 10.0
    irsByKey(List("id2"))(0).asInstanceOf[Double] shouldBe 20.0
    irsByKey(List("id3"))(0).asInstanceOf[Double] shouldBe 30.0
  }

  it should "tiled entity job — before-only rows produce negative sum (delete from empty)" in {
    // Verify that isBefore=true events are processed as deletes (subtract from IR).
    // Three keys each receive a single before-row (delete) with distinct values.
    // A delete on an empty accumulator results in a negative sum.
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val elements = Seq(
      E2ETestMutationEvent("id1", 1, 10.0, 1L, 2L, isBefore = true),
      E2ETestMutationEvent("id2", 1, 20.0, 3L, 4L, isBefore = true),
      E2ETestMutationEvent("id3", 1, 30.0, 5L, 6L, isBefore = true)
    )

    val groupBy = FlinkTestUtils.makeEntityGroupBy(Seq("id"))
    val (job, gbInfo) = buildFlinkJob(groupBy, elements)
    job.runTiledGroupByJob(env).addSink(new CollectSink)
    env.execute("TiledEntityJobBeforeOnly")

    val responses = CollectSink.values.toScala
    responses.forall(_.status) shouldBe true

    val irsByKey = finalIRsPerKey(responses, gbInfo)
    // A delete on an empty IR subtracts the value
    irsByKey(List("id1"))(0).asInstanceOf[Double] shouldBe -10.0
    irsByKey(List("id2"))(0).asInstanceOf[Double] shouldBe -20.0
    irsByKey(List("id3"))(0).asInstanceOf[Double] shouldBe -30.0
  }




  // ---- getAllowedLatenessMs tests (no mini-cluster needed) ----

  "FlinkGroupByStreamingJob.getAllowedLatenessMs" should "default to 2 days for entity source with no prop" in {
    val (job, _) = buildFlinkJob(FlinkTestUtils.makeEntityGroupBy(Seq("id")), Seq.empty)
    job.getAllowedLatenessMs() shouldBe 2L * 24 * 60 * 60 * 1000
  }

  it should "default to 0 for event source with no prop" in {
    val (job, _) = buildFlinkJob(FlinkTestUtils.makeGroupBy(Seq("id")), Seq.empty)
    job.getAllowedLatenessMs() shouldBe 0L
  }

  it should "respect explicit allowed_lateness_seconds prop for entity source" in {
    val (job, _) = buildFlinkJob(FlinkTestUtils.makeEntityGroupBy(Seq("id")), Seq.empty,
                                 props = Map("allowed_lateness_seconds" -> "3600"))
    job.getAllowedLatenessMs() shouldBe 3600000L
  }

  it should "respect explicit allowed_lateness_seconds prop for event source" in {
    val (job, _) = buildFlinkJob(FlinkTestUtils.makeGroupBy(Seq("id")), Seq.empty,
                                 props = Map("allowed_lateness_seconds" -> "120"))
    job.getAllowedLatenessMs() shouldBe 120000L
  }

  private def buildFlinkJob(
      groupBy: GroupBy,
      elements: Seq[E2ETestMutationEvent],
      props: Map[String, String] = Map.empty,
      parallelism: Int = 2
  ): (FlinkGroupByStreamingJob, GroupByServingInfoParsed) = {
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val sparkExpressionEvalFn = new SparkExpressionEvalFn(Encoders.product[E2ETestMutationEvent], query, groupBy.metaData.name, groupBy.dataModel)
    val source = new WatermarkedE2ETestMutationEventSource(elements, sparkExpressionEvalFn)

    val encoder = Encoders.product[E2ETestMutationEvent]
    val outputSchema = new SparkExpressionEval(encoder, query, groupBy.getMetaData.getName, groupBy.dataModel).getOutputSchema
    val outputSchemaDataTypes = outputSchema.fields.map { field =>
      (field.name, SparkConversions.toChrononType(field.name, field.dataType))
    }

    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, groupBy.metaData.name)
    val topicInfo = TopicInfo.parse("kafka://test-topic")
    (new FlinkGroupByStreamingJob(source,
                  outputSchemaDataTypes,
                  writerFn,
                  groupByServingInfoParsed,
                  parallelism,
                  props = props,
                  topicInfo = topicInfo),
     groupByServingInfoParsed)
  }
}
