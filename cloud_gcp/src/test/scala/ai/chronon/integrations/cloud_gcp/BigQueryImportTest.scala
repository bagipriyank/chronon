package ai.chronon.integrations.cloud_gcp

import ai.chronon.api
import ai.chronon.api.planner.MetaDataUtils
import ai.chronon.api.{MetaData, PartitionRange, PartitionSpec, TableDependency}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.SparkTestBase
import com.google.cloud.bigquery._
import org.junit.Assert.assertTrue
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatestplus.mockito.MockitoSugar

class BigQueryImportTest extends SparkTestBase with MockitoSugar {

  private val partitionSpec = PartitionSpec.daily

  override def sparkConfs: Map[String, String] = Map(
    "spark.sql.catalog.test_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.test_catalog.type" -> "hadoop",
    "spark.sql.catalog.test_catalog.warehouse" -> "file:///tmp/chronon/test-warehouse"
  )

  lazy val tableUtils: TableUtils = TableUtils(spark)

  private def createTestStagingQuery(outputTable: String = "test_catalog.test_dataset.test_table"): api.StagingQuery = {
    // Create base metadata
    val baseMetaData = new MetaData()
    baseMetaData.setName("base_staging_query")
    baseMetaData.setOutputNamespace("test_catalog.test_namespace")

    // Use MetaDataUtils to layer the metadata with proper configuration
    implicit val implicitPartitionSpec: PartitionSpec = partitionSpec
    val layeredMetaData = MetaDataUtils.layer(
      baseMetadata = baseMetaData,
      modeName = "test",
      nodeName = "test_staging_query",
      tableDependencies = Seq.empty[TableDependency],
      outputTableOverride = Some(outputTable)
    )

    val stagingQuery = new api.StagingQuery()
    stagingQuery.setMetaData(layeredMetaData)
    stagingQuery.setQuery("SELECT * FROM source_table WHERE ds = '{{ ds }}'")
    stagingQuery.setStartPartition("2024-01-01")
    stagingQuery
  }

  private def mockBigQueryClient(): BigQuery = {
    val mockClient = mock[BigQuery]
    val mockJob = mock[Job]
    val mockJobStatus = mock[JobStatus]
    val mockTable = mock[Table]
    val mockTableId = mock[TableId]

    // Mock successful job execution
    when(mockJob.waitFor()).thenReturn(mockJob)
    when(mockJob.getStatus).thenReturn(mockJobStatus)
    when(mockJobStatus.getError).thenReturn(null)
    when(mockClient.create(any[JobInfo])).thenReturn(mockJob)
    when(mockClient.create(any[TableInfo])).thenReturn(mockTable)
    when(mockTable.getTableId).thenReturn(mockTableId)

    mockClient
  }

  private def mockBigQueryClientWithError(errorMessage: String): BigQuery = {
    val mockClient = mock[BigQuery]
    val mockJob = mock[Job]
    val mockJobStatus = mock[JobStatus]
    val mockError = mock[BigQueryError]

    when(mockJob.waitFor()).thenReturn(mockJob)
    when(mockJob.getStatus).thenReturn(mockJobStatus)
    when(mockJobStatus.getError).thenReturn(mockError)
    when(mockError.getMessage).thenReturn(errorMessage)
    when(mockClient.create(any[JobInfo])).thenReturn(mockJob)

    mockClient
  }

  /**
   * Creates a BigQueryImport instance with compute() overridden to simulate BigQuery export
   * by writing dummy parquet data to the local filesystem.
   */
  private def createMockedBigQueryImport(
      stagingQueryConf: api.StagingQuery,
      endPartition: String,
      mockClient: BigQuery
  ): BigQueryImport = {
    new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient

      override def compute(range: PartitionRange,
                           setups: Seq[String],
                           enableAutoExpand: scala.Option[Boolean],
                           outputLocation: scala.Option[String]): Unit = {
        // Step 1: Simulate BigQuery export by writing dummy data to the expected location
        val exportPath = exportUri(range.start, range.end).stripSuffix(s"/*.${formatStr}")
        import spark.implicits._
        val dummyData = range.partitions.flatMap { partition =>
          Seq((1, s"value1_$partition", partition), (2, s"value2_$partition", partition))
        }.toDF("id", "value", "ds")
        dummyData.write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(exportPath)

        // Step 2: Call parent compute to read from temp and write to Iceberg
        super.compute(range, setups, enableAutoExpand, outputLocation)
      }
    }
  }

  "BigQueryStagingQuery" should "initialize with correct configuration" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    // Verify the class is initialized correctly
    assertTrue("BigQueryStagingQuery should be initialized", bigQueryStagingQuery != null)
  }

  it should "generate correct export data template without setups" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val uri = "gs://test-bucket/test-path/*.parquet"
    val sql = "SELECT * FROM test_table WHERE ds = '2024-01-01'"
    val setups = Seq.empty[String]
    val result = bigQueryStagingQuery.exportDataTemplate(uri, sql, setups)

    assertTrue("Export template should contain URI", result.contains(uri))
    assertTrue("Export template should contain SQL", result.contains(sql))
    assertTrue("Export template should use parquet format", result.contains("format = 'parquet'"))
    assertTrue("Export template should have overwrite = true", result.contains("overwrite = true"))
    assertTrue("Export template should not contain BEGIN/END when no setups", !result.contains("BEGIN"))
  }

  it should "generate correct multi-statement query with setups" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val uri = "gs://test-bucket/test-path/*.parquet"
    val sql = "SELECT * FROM test_table WHERE ds = '2024-01-01'"
    val setups = Seq(
      "CREATE TEMP TABLE temp_data AS SELECT * FROM source_data",
      "CREATE TEMP FUNCTION process_data(input STRING) AS (UPPER(input))"
    )
    val result = bigQueryStagingQuery.exportDataTemplate(uri, sql, setups)

    assertTrue("Multi-statement query should contain BEGIN", result.contains("BEGIN"))
    assertTrue("Multi-statement query should contain END;", result.contains("END;"))
    assertTrue("Multi-statement query should contain first setup", result.contains(setups(0)))
    assertTrue("Multi-statement query should contain second setup", result.contains(setups(1)))
    assertTrue("Multi-statement query should contain URI", result.contains(uri))
    assertTrue("Multi-statement query should contain SQL", result.contains(sql))
    assertTrue("Multi-statement query should use parquet format", result.contains("format = 'parquet'"))
    assertTrue("Multi-statement query should have overwrite = true", result.contains("overwrite = true"))

    // Verify that setup statements are properly terminated with semicolons
    assertTrue("Setup statements should end with semicolon", result.contains(setups(0) + ";"))
    assertTrue("Setup statements should end with semicolon", result.contains(setups(1) + ";"))
  }

  it should "generate correct export URI" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val startPartition = "2024-01-01"
    val endPartitionValue = "2024-01-03"
    val result = bigQueryStagingQuery.exportUri(startPartition, endPartitionValue)

    assertTrue("Export URI should contain start partition", result.contains(startPartition))
    assertTrue("Export URI should contain end partition", result.contains(endPartitionValue))
    assertTrue("Export URI should end with parquet extension", result.endsWith("*.parquet"))
    assertTrue("Export URI should contain temp export prefix path", result.contains("export/"))
  }

  it should "successfully compute with single partition" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()
    val bigQueryImport = createMockedBigQueryImport(stagingQueryConf, endPartition, mockClient)

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    // This should not throw an exception
    bigQueryImport.compute(range, setups, Some(true))

    // Verify that BigQuery client export job was called once (not per partition, but for the full range)
    verify(mockClient, org.mockito.Mockito.times(1)).create(any[JobInfo])
    // No longer creates external table via BigQuery - creates Iceberg table via TableUtils instead
  }

  it should "successfully compute with multiple partitions" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()
    val bigQueryImport = createMockedBigQueryImport(stagingQueryConf, endPartition, mockClient)

    val range = PartitionRange("2024-01-01", "2024-01-03")(partitionSpec)
    val setups = Seq.empty[String]

    bigQueryImport.compute(range, setups, Some(true))

    // Verify BigQuery client was called once for the full range (not per partition)
    verify(mockClient, org.mockito.Mockito.times(1)).create(any[JobInfo])
    // No longer creates external table via BigQuery - creates Iceberg table via TableUtils instead
  }

  it should "handle BigQuery job errors gracefully" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val errorMessage = "BigQuery job failed"
    val mockClient = mockBigQueryClientWithError(errorMessage)

    val bigQueryImport = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val bigQueryClient: BigQuery = mockClient
    }

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    // This should throw a RuntimeException with the BigQuery error message
    val exception = intercept[RuntimeException] {
      bigQueryImport.compute(range, setups, Some(true))
    }

    assertTrue("Exception should contain BigQuery error message", exception.getMessage.contains(errorMessage))
  }

  it should "export to temp location and write to Iceberg table" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()

    val bigQueryImport = createMockedBigQueryImport( stagingQueryConf, endPartition, mockClient )

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    bigQueryImport.compute(range, setups, Some(true))

    // Verify that export job was called
    verify(mockClient, org.mockito.Mockito.times(1)).create(any[JobInfo])

    // The new implementation exports to temp, reads back, and writes to Iceberg via TableUtils
    // No external table is created via BigQuery client
  }

  it should "use correct warehouse location for temp export prefix" in {
    val customWarehouse = "gs://custom-warehouse/data"

    // Update the global spark configuration
    spark.conf.set("spark.sql.catalog.custom_catalog.warehouse", customWarehouse)

    val stagingQueryConf = createTestStagingQuery("custom_catalog.test_dataset.test_table")
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val tempPrefix = bigQueryStagingQuery.tempExportPrefix

    assertTrue("Temp export prefix should contain custom warehouse location",
               tempPrefix.contains(customWarehouse.stripSuffix("/")))
    assertTrue("Temp export prefix should contain sanitized table name",
               tempPrefix.contains("export/custom_catalog_test_dataset_test_table"))
  }

  it should "handle null enableAutoExpand parameter" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()

    val bigQueryImport = createMockedBigQueryImport(stagingQueryConf, endPartition, mockClient)

    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq.empty[String]

    // This should not throw an exception even with null enableAutoExpand
    bigQueryImport.compute(range, setups, None)

    verify(mockClient, org.mockito.Mockito.times(1)).create(any[JobInfo])
    // No longer creates external table via BigQuery
  }

  it should "properly sanitize table names in paths" in {
    val stagingQueryConf = createTestStagingQuery("test_catalog.`test-dataset`.test_table")
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = createMockedBigQueryImport(stagingQueryConf, endPartition, mockBigQueryClient())

    val tempPrefix = bigQueryStagingQuery.tempExportPrefix

    // The sanitize method should replace dots and hyphens with underscores
    assertTrue("Temp export prefix should contain sanitized table name",
               tempPrefix.contains("test_catalog__test_dataset__test_table"))
  }

  it should "successfully compute with setups and multi-statement query" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mockBigQueryClient()

    val bigQueryImport = createMockedBigQueryImport(stagingQueryConf, endPartition, mockClient)


    val range = PartitionRange("2024-01-01", "2024-01-01")(partitionSpec)
    val setups = Seq(
      "CREATE TEMP TABLE temp_data AS SELECT * FROM source_data",
      "CREATE TEMP FUNCTION process_data(input STRING) AS (UPPER(input))"
    )

    // This should not throw an exception and should execute the multi-statement query
    bigQueryImport.compute(range, setups, Some(true))

    // Verify that BigQuery export job was called
    verify(mockClient, org.mockito.Mockito.times(1)).create(any[JobInfo])
    // No longer creates external table via BigQuery
  }

  it should "properly substitute variables in setup statements" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"
    val mockClient = mock[BigQuery]
    val mockJob = mock[Job]
    val mockJobStatus = mock[JobStatus]

    // Capture the JobInfo that gets passed to create()
    var capturedJobInfo: JobInfo = null
    when(mockJob.waitFor()).thenReturn(mockJob)
    when(mockJob.getStatus).thenReturn(mockJobStatus)
    when(mockJobStatus.getError).thenReturn(null)
    when(mockClient.create(any[JobInfo])).thenAnswer { invocation =>
      capturedJobInfo = invocation.getArgument(0).asInstanceOf[JobInfo]
      mockJob
    }

    val bigQueryImport = createMockedBigQueryImport(stagingQueryConf, endPartition, mockClient)

    val range = PartitionRange("2024-01-15", "2024-01-15")(partitionSpec)
    val setups = Seq(
      "CREATE TEMP TABLE temp_data AS SELECT * FROM source_data WHERE partition_date = {{ start_date }}",
      "CREATE TEMP TABLE temp_end AS SELECT * FROM end_data WHERE end_date = {{ end_date }}"
    )

    bigQueryImport.compute(range, setups, Some(true))

    // Verify that the JobInfo contains substituted setup statements
    assertTrue("JobInfo should be captured", capturedJobInfo != null)
    val queryConfig = capturedJobInfo.getConfiguration.asInstanceOf[QueryJobConfiguration]
    val renderedQuery = queryConfig.getQuery

    // Check that variables were properly substituted in setup statements
    assertTrue("Setup should contain substituted start date",
               renderedQuery.contains("partition_date = '2024-01-15'"))
    assertTrue("Setup should contain substituted end date",
               renderedQuery.contains("end_date = '2024-01-15'"))
    assertTrue("Setup should not contain unsubstituted variables",
               !renderedQuery.contains("{{ start_date }}") && !renderedQuery.contains("{{ end_date }}"))
  }

  it should "render setup SQL with complex variable substitutions" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryStagingQuery = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    val uri = "gs://test-bucket/test-path/*.parquet"
    val sql = "SELECT * FROM test_table WHERE ds = '{{ start_date }}' AND ds <= '{{ end_date }}'"
    val setups = Seq(
      "CREATE TEMP TABLE partition_data_{{ start_date }} AS (SELECT * FROM source WHERE date_col = '{{ start_date }}')",
      "CREATE TEMP FUNCTION get_end_date() AS ('{{ end_date }}')",
      "CREATE OR REPLACE VIEW test_view AS SELECT * FROM base_table WHERE partition >= '{{ start_date }}' AND partition <= '{{ end_date }}'"
    )
    val result = bigQueryStagingQuery.exportDataTemplate(uri, sql, setups)

    // Verify the structure contains setup statements with variable placeholders
    assertTrue("Template should contain BEGIN block", result.contains("BEGIN"))
    assertTrue("Template should contain END block", result.contains("END;"))
    assertTrue("Template should contain first setup with variables", result.contains("CREATE TEMP TABLE partition_data_{{ start_date }}"))
    assertTrue("Template should contain second setup with variables", result.contains("CREATE TEMP FUNCTION get_end_date() AS ('{{ end_date }}')"))
    assertTrue("Template should contain third setup with variables", result.contains("CREATE OR REPLACE VIEW test_view"))
    assertTrue("Template should contain export data section", result.contains("EXPORT DATA"))
    assertTrue("Template should contain main SQL with variables", result.contains("SELECT * FROM test_table WHERE ds = '{{ start_date }}' AND ds <= '{{ end_date }}'"))

    // Verify each setup statement is properly terminated
    setups.foreach { setup =>
      assertTrue(s"Setup '$setup' should be terminated with semicolon", result.contains(setup + ";"))
    }
  }

  "cleanupTempDirectory" should "successfully delete temp directory" in {

    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryImport = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    // Get the temp export prefix to verify cleanup
    val tempPrefix = bigQueryImport.tempExportPrefix

    // Create some test data in a subdirectory (simulating BigQuery export structure)
    import spark.implicits._
    val testData = Seq((1, "test"), (2, "data")).toDF("id", "value")
    testData.write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(s"${tempPrefix}/2024-01-01_to_2024-01-01")

    // Verify directory exists
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = new org.apache.hadoop.fs.Path(tempPrefix).getFileSystem(hadoopConf)
    val path = new org.apache.hadoop.fs.Path(tempPrefix)
    assertTrue("Temp directory should exist before cleanup", fs.exists(path))

    // Invoke cleanup directly
    bigQueryImport.cleanupTempDirectory()

    // Verify temp directory was deleted
    assertTrue("Temp directory should be deleted after cleanup", !fs.exists(path))
  }

  it should "not throw exception when cleanup fails" in {
    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    // Create a BigQueryImport instance with an invalid temp prefix that will cause write to fail
    val bigQueryImport = new BigQueryImport(stagingQueryConf, endPartition, tableUtils) {
      override private[cloud_gcp] lazy val tempExportPrefix = {
        // Use an invalid path that will cause the write operation to fail
        "invalid://bad-path/that-does-not-exist"
      }
    }

    // This should NOT throw an exception even though cleanup will fail
    // The method should catch the exception and only log a warning
    try {
      bigQueryImport.cleanupTempDirectory()
      // If we get here, the test passes - no exception was thrown
      assertTrue("Cleanup should not throw exception on failure", true)
    } catch {
      case _: Throwable =>
        // If an exception is thrown, the test fails
        assertTrue("Cleanup should not throw exception on failure, but it did", false)
    }
  }

  it should "handle cleanup when temp directory doesn't exist" in {

    val stagingQueryConf = createTestStagingQuery()
    val endPartition = "2024-01-31"

    val bigQueryImport = new BigQueryImport(stagingQueryConf, endPartition, tableUtils)

    // Don't create any data in temp directory - just invoke cleanup on non-existent directory
    // This should succeed without errors (just logs that directory doesn't exist)
    try {
      bigQueryImport.cleanupTempDirectory()
      assertTrue("Cleanup should succeed even when directory doesn't exist", true)
    } catch {
      case _: Throwable =>
        assertTrue("Cleanup should not throw exception when directory doesn't exist", false)
    }
  }

}
