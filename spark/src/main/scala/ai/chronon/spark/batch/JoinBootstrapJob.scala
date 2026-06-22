package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api.{Constants, DateRange, MetaData, PartitionRange, PartitionSpec, StructField, StructType}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.planner.JoinBootstrapNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.{coalescedJoin, set_add}
import ai.chronon.spark.{BootstrapInfo, JoinUtils}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, typedLit}
import org.slf4j.{Logger, LoggerFactory}
import ai.chronon.spark.catalog.TableUtils

/** Runs after the `SourceJob` and produces boostrap table that is then used in the final join. Unique per join, whereas
  * `SourceJob` output is shared across all joins.
  *
  * Note for orchestrator: This needs to run iff there are bootstraps or external parts to the join (applies additional
  * columns that may be used in derivations). Otherwise, the left source table can be used directly in final join.
  */
class JoinBootstrapJob(node: JoinBootstrapNode,
                       metaData: MetaData,
                       range: DateRange,
                       outputLocation: Option[String] = None)(implicit tableUtils: TableUtils) {
  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val join = node.join
  private val dateRange = range.toPartitionRange
  private val leftSourceTable = JoinUtils.computeFullLeftSourceTableName(join)

  // Use the node's metadata output table
  private val outputTable = metaData.outputTable

  def run(): Unit = tableUtils.withJobDescription(s"JoinBootstrapJob(${join.metaData.name}) $dateRange") {
    // Runs the bootstrap query and produces an output table specific to the `left` side of the Join
    // LeftSourceTable is the same as the SourceJob output table for the Left.
    // `f"${source.table}_${ThriftJsonCodec.md5Digest(sourceWithFilter)}"` Logic should  be computed by orchestrator
    // and passed to both jobs
    val leftDf = tableUtils.scanDf(query = null, table = leftSourceTable, range = Some(dateRange))

    val bootstrapInfo = BootstrapInfo.from(join, dateRange, tableUtils, Option(leftDf.schema))

    computeBootstrapTable(leftDf = leftDf, bootstrapInfo = bootstrapInfo, outputLocation = outputLocation)
  }

  def computeBootstrapTable(leftDf: DataFrame,
                            bootstrapInfo: BootstrapInfo,
                            tableProps: Map[String, String] = null,
                            outputLocation: Option[String] = None): DataFrame = {

    val bootstrapTable: String = outputTable

    def validateReservedColumns(df: DataFrame, table: String, columns: Seq[String]): Unit = {
      val reservedColumnsContained = columns.filter(df.schema.fieldNames.contains)
      assert(
        reservedColumnsContained.isEmpty,
        s"Table $table contains columns ${reservedColumnsContained.prettyInline} which are reserved by Chronon."
      )
    }

    val startMillis = System.currentTimeMillis()

    // verify left table does not have reserved columns
    validateReservedColumns(leftDf, join.left.table, Seq(Constants.BootstrapHash, Constants.MatchedHashes))

    val parts = Option(join.bootstrapParts)
      .map(_.toScala)
      .getOrElse(Seq())

    val initDf = leftDf
      // initialize an empty matched_hashes column for the purpose of later processing
      .withColumn(Constants.MatchedHashes, typedLit[Array[String]](null))

    val joinedDf = parts.foldLeft(initDf) { case (partialDf, part) =>
      logger.info(s"\nProcessing Bootstrap from table ${part.table} for range $range")

      val bootstrapRange = if (part.isSetQuery) {
        dateRange.intersect(PartitionRange(part.startPartition, part.endPartition))
      } else {
        dateRange
      }
      if (!bootstrapRange.valid) {
        logger.info(s"partition range of bootstrap table ${part.table} is beyond unfilled range")
        partialDf
      } else {
        var bootstrapDf =
          tableUtils.scanDf(part.query,
                            part.table,
                            Some(Map(part.query.partitionSpec(tableUtils.partitionSpec).column -> null)),
                            range = Some(bootstrapRange))

        // attach semantic_hash for either log or regular table bootstrap
        validateReservedColumns(bootstrapDf, part.table, Seq(Constants.BootstrapHash, Constants.MatchedHashes))
        if (bootstrapDf.columns.contains(Constants.SchemaHash)) {
          bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, col(Constants.SchemaHash))
        } else {
          bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, lit(part.semanticHash))
        }

        // include only necessary columns. in particular,
        // this excludes columns that are NOT part of Join's output (either from GB or external source)
        val includedColumns = bootstrapDf.columns
          .filter(
            bootstrapInfo.fieldNames ++ part.keys(join, tableUtils.partitionColumn)
              ++ Seq(Constants.BootstrapHash, tableUtils.partitionColumn))
          .sorted

        bootstrapDf = bootstrapDf
          .select(includedColumns.map(col): _*)
          // TODO: allow customization of deduplication logic
          .dropDuplicates(part.keys(join, tableUtils.partitionColumn).toArray)

        coalescedJoin(partialDf, bootstrapDf, part.keys(join, tableUtils.partitionColumn))
          // as part of the left outer join process, we update and maintain matched_hashes for each record
          // that summarizes whether there is a join-match for each bootstrap source.
          // later on we use this information to decide whether we still need to re-run the backfill logic
          .withColumn(Constants.MatchedHashes, set_add(col(Constants.MatchedHashes), col(Constants.BootstrapHash)))
          .drop(Constants.BootstrapHash)
      }
    }

    println(s"JoinedDF schema: ${joinedDf.schema}")

    // include all external fields if not already bootstrapped
    val enrichedDf = padExternalFields(joinedDf, bootstrapInfo)

    println(s"EnrichedDF schema: ${enrichedDf.schema}")

    // set autoExpand = true since log table could be a bootstrap part
    enrichedDf.save(bootstrapTable, tableProps, autoExpand = true, outputLocation = outputLocation)

    val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
    logger.info(s"Finished computing bootstrap table $bootstrapTable in $elapsedMins minutes")

    tableUtils.scanDf(query = null, table = bootstrapTable, range = Some(dateRange))
  }

  /*
   * For all external fields that are not already populated during the bootstrap step, fill in NULL.
   * This is so that if any derivations depend on the these external fields, they will still pass and not complain
   * about missing columns. This is necessary when we directly bootstrap a derived column and skip the base columns.
   */
  private def padExternalFields(bootstrapDf: DataFrame, bootstrapInfo: BootstrapInfo): DataFrame = {

    val nonContextualFields = toSparkSchema(
      bootstrapInfo.externalParts
        .filter(!_.externalPart.isContextual)
        .flatMap(part => part.keySchema ++ part.valueSchema))
    val contextualFields = toSparkSchema(
      bootstrapInfo.externalParts.filter(_.externalPart.isContextual).flatMap(_.keySchema))

    def withNonContextualFields(df: DataFrame): DataFrame = padFields(df, nonContextualFields)

    // Ensure keys and values for contextual fields are consistent even if only one of them is explicitly bootstrapped
    def withContextualFields(df: DataFrame): DataFrame =
      contextualFields.foldLeft(df) { case (df, field) =>
        var newDf = df
        if (!newDf.columns.contains(field.name)) {
          newDf = newDf.withColumn(field.name, lit(null).cast(field.dataType))
        }
        val prefixedName = s"${Constants.ContextualPrefix}_${field.name}"
        if (!newDf.columns.contains(prefixedName)) {
          newDf = newDf.withColumn(prefixedName, lit(null).cast(field.dataType))
        }
        newDf
          .withColumn(field.name, coalesce(col(field.name), col(prefixedName)))
          .withColumn(prefixedName, coalesce(col(field.name), col(prefixedName)))
      }

    withContextualFields(withNonContextualFields(bootstrapDf))
  }

  private def padFields(df: DataFrame, structType: sql.types.StructType): DataFrame = {
    structType.foldLeft(df) { case (df, field) =>
      if (df.columns.contains(field.name)) {
        df
      } else {
        df.withColumn(field.name, lit(null).cast(field.dataType))
      }
    }
  }

  private def toSparkSchema(fields: Seq[StructField]): sql.types.StructType =
    SparkConversions.fromChrononSchema(StructType("", fields.toArray))

}
