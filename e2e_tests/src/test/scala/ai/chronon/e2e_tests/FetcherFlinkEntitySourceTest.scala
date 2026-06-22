package ai.chronon.e2e_tests

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.{FlagStore, FlagStoreConstants}
import ai.chronon.spark.GroupByUpload
import ai.chronon.spark.batch.ModularMonolith
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import ai.chronon.flink.test.CollectSink
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration => ScalaDuration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}

/** End-to-end integration tests for entity (CDC/mutation) tiled fetch.
  * The Spark-based non-tiled analogue is FetcherDeterministicTest.
  *
  *  1. Spark:  write snapshot + mutation tables, run GroupByUpload for the batch IR.
  *  2. Flink:  run FlinkGroupByStreamingJob (tiled) over mutation rows, capture tile WriteResponses.
  *  3. Bridge: put tile bytes into InMemoryKvStore (same store the fetcher reads).
  *  4. Fetch:  compare online feature values against expected values.
  */
class FetcherFlinkEntitySourceTest extends SparkTestBase with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build
  )

  before {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
  }

  private val ns  = "entity_tiled_integration"
  private val ns2 = "entity_tiled_cross_window"

  it should "entity tiled online fetch matches offline join ground truth" in {
    implicit val tableUtils: TableUtils = TableUtils(spark)
    implicit val ec: ExecutionContext   = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

    // -----------------------------------------------------------------------
    // Step 1: Write Spark tables + offline ground truth
    // -----------------------------------------------------------------------
    createDatabase(ns)
    FetcherFlinkTestUtils.writeSparkTables(ns)

    val groupBy = FetcherFlinkTestUtils.makeGroupBy(ns)

    spark.createDataFrame(FetcherFlinkTestUtils.queryRows.toJava,
                          SparkConversions.fromChrononSchema(FetcherFlinkTestUtils.eventSchema))
         .save(s"$ns.${FetcherFlinkTestUtils.eventSchema.name}")

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(selects = Builders.Selects("listing_id", "ts"), startPartition = "2021-04-08"),
        table = s"$ns.${FetcherFlinkTestUtils.eventSchema.name}"
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData  = Builders.MetaData(
        name = "integration_test.entity_tiled_join", namespace = ns, team = "chronon")
    )

    // -----------------------------------------------------------------------
    // Step 2: Run offline join (ground truth)
    // -----------------------------------------------------------------------
    ModularMonolith.run(joinConf,
      new DateRange().setStartDate("2021-04-10").setEndDate(FetcherFlinkTestUtils.endDs))(tableUtils)
    val offlineDf = tableUtils.sql(
      s"SELECT * FROM ${joinConf.metaData.outputTable} WHERE ds='${FetcherFlinkTestUtils.endDs}'")
    offlineDf.cache()

    // -----------------------------------------------------------------------
    // Step 3: Batch upload + metadata registration
    // -----------------------------------------------------------------------
    val kvStoreFunc     = () => OnlineUtils.buildInMemoryKVStore("EntityTiledIntegrationTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi         = new MockApi(kvStoreFunc, ns)

    GroupByUpload.run(groupBy, FetcherFlinkTestUtils.batchEnd, Some(tableUtils))
    inMemoryKvStore.bulkPut(groupBy.metaData.uploadTable, groupBy.batchDataset, null)
    inMemoryKvStore.create(groupBy.streamingDataset)
    inMemoryKvStore.create(MetadataDataset)
    Await.result(new MetadataStore(FetchContext(inMemoryKvStore)).putJoinConf(joinConf), ScalaDuration(30, SECONDS))

    // -----------------------------------------------------------------------
    // Step 4: Run Flink tiled job, capture WriteResponses
    // -----------------------------------------------------------------------
    val writeResponses = FetcherFlinkTestUtils.runFlinkTiledJob(groupBy, mockApi)
    writeResponses should not be empty
    writeResponses.forall(_.status) shouldBe true

    // -----------------------------------------------------------------------
    // Step 5: Bridge — put Flink tile bytes into InMemoryKvStore
    // -----------------------------------------------------------------------
    inMemoryKvStore.multiPut(FetcherFlinkTestUtils.tileResponsesToPutRequests(writeResponses))

    // -----------------------------------------------------------------------
    // Step 6: Fetch with tiling enabled
    // -----------------------------------------------------------------------
    val tilingMockApi = new MockApi(kvStoreFunc, ns)
    tilingMockApi.setFlagStore(new FlagStore {
      override def isSet(flagName: String, attrs: java.util.Map[String, String]): java.lang.Boolean =
        flagName == FlagStoreConstants.TILING_ENABLED
    })
    val fetcher = tilingMockApi.buildFetcher(debug = true)

    val offlineRows  = offlineDf.collect()
    val listingIdIdx = offlineDf.schema.fieldIndex("listing_id")
    val tsIdx        = offlineDf.schema.fieldIndex("ts")
    val requests     = offlineRows.map { row =>
      Request(joinConf.metaData.name,
              Map("listing_id" -> row.get(listingIdIdx).asInstanceOf[AnyRef]),
              Some(row.get(tsIdx).asInstanceOf[Long]))
    }

    val responses = Await.result(fetcher.fetchJoin(requests.toSeq), ScalaDuration(60, SECONDS))

    // -----------------------------------------------------------------------
    // Step 7: Compare online vs offline
    // -----------------------------------------------------------------------
    responses.length shouldBe offlineRows.length

    val sumCol = offlineDf.schema.fieldNames.find(_.endsWith("rating_sum")).getOrElse(
      fail(s"No rating_sum column in: ${offlineDf.schema.fieldNames.mkString(", ")}"))
    val avgCol = offlineDf.schema.fieldNames.find(_.endsWith("rating_average_1d")).getOrElse(
      fail(s"No rating_average_1d column in: ${offlineDf.schema.fieldNames.mkString(", ")}"))

    val offlineByKeyTs = offlineRows.map { row =>
      (row.get(listingIdIdx).asInstanceOf[AnyRef], row.get(tsIdx).asInstanceOf[Long]) -> row
    }.toMap

    responses.foreach { response =>
      response.values.isSuccess shouldBe true
      val online = response.values.get
      val key    = response.request.keys("listing_id")
      val ts     = response.request.atMillis.get

      def onlineVal(col: String): Option[AnyRef] =
        online.get(col).orElse(online.find(_._1.endsWith(col.stripPrefix("integration_test_entity_tiled_gb_"))).map(_._2))

      offlineByKeyTs.get((key, ts)).foreach { offlineRow =>
        val offlineSum = offlineRow.getAs[Any](sumCol)
        val offlineAvg = offlineRow.getAs[Any](avgCol)

        if (offlineSum != null)
          withClue(s"key=$key ts=$ts sumCol=$sumCol") {
            onlineVal(sumCol) shouldBe Some(offlineSum.asInstanceOf[AnyRef])
          }
        if (offlineAvg != null)
          onlineVal(avgCol).foreach { v =>
            v.asInstanceOf[Double] shouldBe offlineAvg.asInstanceOf[Double] +- 1e-6
          }
      }
    }
  }

  /** Mutations land in two tile windows that straddle the 1-day aggregation boundary.
    *
    * GroupBy: rating_sum (unwindowed) + rating_average_1d (1-day window).
    * Tile hop for a 1d window = 1 hour (FiveMinuteResolution).
    * batchEndTs = 2021-04-10 00:00 UTC (after(endDs="2021-04-09")); queryTs = 2021-04-11 12:00 UTC.
    * 1d window start at query time = floor(queryTs - 24h, 1h) = 2021-04-10 12:00 UTC.
    *
    * Tile A (ts=2021-04-10 06:00): after batchEnd but outside the 1d window → rating_sum only.
    * Tile B (ts=2021-04-11 06:00): after batchEnd and inside the 1d window → both aggregations.
    *   Tile B contains an insert (rating=5) then an update (before=5, after=7) → net tile IR = +7.
    * Batch collapsed IR: one snapshot row at 2021-04-07 with rating=2.
    *
    * Expected at queryTs:
    *   rating_sum         = 2(batch) + 3(tileA) + 7(tileB) = 12
    *   rating_average_1d  = 7.0  (only tileB falls inside the 1d window)
    */
  it should "only tiles within the aggregation window contribute to windowed aggregations" in {
    implicit val tableUtils: TableUtils = TableUtils(spark)
    implicit val ec: ExecutionContext   = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

    createDatabase(ns2)
    FetcherFlinkTestUtils.writeCrossWindowSnapshotRow(ns2)

    val groupBy = FetcherFlinkTestUtils.makeGroupBy(ns2)

    val kvStoreFunc     = () => OnlineUtils.buildInMemoryKVStore("EntityTiledCrossWindowTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi         = new MockApi(kvStoreFunc, ns2)

    GroupByUpload.run(groupBy, FetcherFlinkTestUtils.batchEnd, Some(tableUtils))
    inMemoryKvStore.bulkPut(groupBy.metaData.uploadTable, groupBy.batchDataset, null)
    inMemoryKvStore.create(groupBy.streamingDataset)
    inMemoryKvStore.create(MetadataDataset)

    val writeResponses = FetcherFlinkTestUtils.runFlinkTiledJobWithEvents(
      groupBy, mockApi, FetcherFlinkTestUtils.crossWindowMutationEvents)
    writeResponses should not be empty
    writeResponses.forall(_.status) shouldBe true

    inMemoryKvStore.multiPut(FetcherFlinkTestUtils.tileResponsesToPutRequests(writeResponses))

    val tilingMockApi = new MockApi(kvStoreFunc, ns2)
    tilingMockApi.setFlagStore(new FlagStore {
      override def isSet(flagName: String, attrs: java.util.Map[String, String]): java.lang.Boolean =
        flagName == FlagStoreConstants.TILING_ENABLED
    })
    val fetcher = tilingMockApi.buildFetcher(debug = true)

    val responses = Await.result(
      fetcher.fetchGroupBys(Seq(Request(
        groupBy.metaData.name,
        Map("listing_id" -> FetcherFlinkTestUtils.crossWindowListingId.asInstanceOf[AnyRef]),
        Some(FetcherFlinkTestUtils.crossWindowQueryTs)
      ))),
      ScalaDuration(60, SECONDS)
    )

    responses should have size 1
    val values = responses.head.values.get

    def col(suffix: String): AnyRef =
      values.find(_._1.endsWith(suffix)).map(_._2).getOrElse(fail(s"missing column ending in $suffix"))

    withClue("rating_sum should include batch + both tiles regardless of window") {
      col("rating_sum") shouldBe 12.asInstanceOf[AnyRef]
    }
    withClue("rating_average_1d should include only the tile inside the 1d window") {
      col("rating_average_1d").asInstanceOf[Double] shouldBe 7.0 +- 1e-6
    }
  }
}
