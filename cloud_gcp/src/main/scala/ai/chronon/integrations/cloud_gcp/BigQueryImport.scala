package ai.chronon.integrations.cloud_gcp

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.PartitionRange
import ai.chronon.api.ScalaJavaConversions.{IterableOps, MapOps}
import ai.chronon.spark.batch.StagingQuery
import ai.chronon.spark.catalog.{Format, TableUtils}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, JobInfo, QueryJobConfiguration}

import java.util.UUID
import scala.util.{Failure, Success, Try}

class BigQueryImport(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils)
    extends StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {

  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private[cloud_gcp] lazy val bigQueryClient: BigQuery = bqOptions.getService

  private[cloud_gcp] val formatStr = "parquet"

  private[cloud_gcp] lazy val warehouseLocation = {
    val catalogName = Format.getCatalog(outputTable)(tableUtils.sparkSession)
    tableUtils.sparkSession.sessionState.conf
      .getConfString(s"spark.sql.catalog.${catalogName}.warehouse")
      .stripSuffix("/")
  }

  private[cloud_gcp] lazy val tempExportPrefix = {
    s"${warehouseLocation}/export/${outputTable.sanitize}_${UUID.randomUUID().toString}"
  }

  private[cloud_gcp] def exportUri(startPartition: String, endPartition: String) =
    s"${tempExportPrefix}/${startPartition}_to_${endPartition}/*.${formatStr}"

  private[cloud_gcp] def exportDataTemplate(uri: String, sql: String, setups: Seq[String]): String = {

    // Requirements for the sql string:
    // `ds` cannot be part of the projection, it is reserved for chronon.
    // It can be part of the WHERE clause.
    val setupStatements = setups.map(setup => s"${setup};").mkString("\n")
    val multiStatementQuery = if (setups.nonEmpty) {
      s"""BEGIN
         |${setupStatements}
         |
         |EXPORT DATA
         |  OPTIONS (
         |    uri = '${uri}',
         |    format = '${formatStr}',
         |    overwrite = true
         |    )
         |AS (
         |   ${sql}
         |);
         |END;""".stripMargin
    } else {
      f"""
       |EXPORT DATA
       |  OPTIONS (
       |    uri = '${uri}',
       |    format = '${formatStr}',
       |    overwrite = true
       |    )
       |AS (
       |   ${sql}
       |);
       |""".stripMargin
    }
    multiStatementQuery
  }

  // outputLocation (from --output-location) is not honored here: this importer exports to its own
  // BigQuery temp location, not via the generic df.save path. The param exists to match the base.
  override def compute(range: PartitionRange,
                       setups: Seq[String],
                       enableAutoExpand: Option[Boolean],
                       outputLocation: Option[String]): Unit = {
    // Step 1: Export data for the full range to a temp location
    val renderedQuery =
      StagingQuery.substitute(
        tableUtils,
        stagingQueryConf.query,
        range.start,
        range.end,
        endPartition
      )
    val tempUri = exportUri(range.start, range.end)
    val renderedSetups = setups.map(s =>
      StagingQuery.substitute(
        tableUtils,
        s,
        range.start,
        range.end,
        endPartition
      ))
    val exportTemplate =
      exportDataTemplate(tempUri, renderedQuery, renderedSetups)
    logger.info(s"Rendered Staging Query to run is:\n$exportTemplate")
    val exportConf = QueryJobConfiguration.of(exportTemplate)
    val exportJobTry = Try {
      val job = bigQueryClient.create(JobInfo.of(exportConf))
      val jobAfterWait = job.waitFor()
      if (jobAfterWait == null) {
        Failure(new RuntimeException("BigQuery job disappeared after submission"))
      } else {
        scala.Option(jobAfterWait.getStatus.getError) match {
          case Some(err) => Failure(new RuntimeException(s"Error exporting data to BigQuery ${err.getMessage}"))
          case None      => Success(jobAfterWait)
        }
      }
    }.flatten

    exportJobTry match {
      case Success(_) =>
        logger.info(
          s"Successfully exported data for range: ${range.start} to ${range.end} to temp location: ${tempExportPrefix}")
      case Failure(exception) =>
        throw exception
    }

    // Step 2: Read the parquet data from temp location and write to final Iceberg table via TableUtils
    try {
      val readPath = exportUri(range.start, range.end).stripSuffix(s"/*.${formatStr}")
      logger.info(s"Reading data from temp location: ${readPath}")
      val df = tableUtils.sparkSession.read.parquet(readPath)

      // Get partition columns from the staging query metadata
      val partitionCols: Seq[String] =
        Seq(range.partitionSpec.column) ++
          (Option(stagingQueryConf.metaData.additionalOutputPartitionColumns)
            .map(_.toScala)
            .getOrElse(Seq.empty))

      val tableProps = Option(stagingQueryConf.metaData.tableProperties)
        .map(_.toScala.toMap)
        .getOrElse(Map.empty[String, String])

      logger.info(s"Writing data to Iceberg table: $outputTable with partitions: ${partitionCols.mkString(", ")}")
      tableUtils.insertPartitions(
        df = df,
        tableName = outputTable,
        tableProperties = tableProps,
        partitionColumns = partitionCols.toList,
        autoExpand = enableAutoExpand.getOrElse(false)
      )

      logger.info(s"Successfully wrote data to Iceberg table $outputTable for range: $range")
    } catch {
      case ex: Throwable =>
        logger.error(s"Error writing to Iceberg table $outputTable", ex)
        throw ex
    } finally {
      // Step 3: Clean up temp directory
      cleanupTempDirectory()
    }

    logger.info(s"Finished writing Staging Query data to $outputTable")
  }

  private[cloud_gcp] def cleanupTempDirectory(): Unit = {
    try {
      logger.info(s"Cleaning up temp directory: ${tempExportPrefix}")
      val hadoopConf = tableUtils.sparkSession.sparkContext.hadoopConfiguration
      val fs = new org.apache.hadoop.fs.Path(tempExportPrefix).getFileSystem(hadoopConf)
      val path = new org.apache.hadoop.fs.Path(tempExportPrefix)
      if (fs.exists(path)) {
        fs.delete(path, true)
        logger.info(s"Successfully deleted temp directory: ${tempExportPrefix}")
      } else {
        logger.info(s"Temp directory does not exist: ${tempExportPrefix}")
      }
    } catch {
      case ex: Throwable =>
        logger.warn(s"Failed to cleanup temp directory ${tempExportPrefix}: ${ex.getMessage}")
      // Don't throw, just log the warning as cleanup failure shouldn't fail the entire job
    }
  }

}
