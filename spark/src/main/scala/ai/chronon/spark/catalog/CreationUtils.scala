package ai.chronon.spark.catalog

import org.apache.spark.sql.types.StructType

object CreationUtils {

  val ALLOWED_TABLE_TYPES = List("iceberg", "delta", "hive", "parquet", "hudi")

  /** Escapes a string value for use in SQL by doubling single quotes.
    * This follows the SQL standard for escaping string literals.
    *
    * @param value The string value to escape
    * @return The escaped string safe for SQL string literals
    */
  def escapeSqlStringValue(value: String): String = {
    value.replace("'", "''")
  }

  def createTableSql(tableName: String,
                     schema: StructType,
                     partitionColumns: List[String],
                     tableProperties: Map[String, String],
                     tableTypeString: String,
                     outputLocation: Option[String] = None,
                     tableLocationLeaf: Option[String] = None): String = {

    require(
      tableTypeString.isEmpty || ALLOWED_TABLE_TYPES.contains(tableTypeString.toLowerCase),
      s"Invalid table type: ${tableTypeString}. Must be empty OR one of: ${ALLOWED_TABLE_TYPES}"
    )

    // A provider-less CREATE (no USING clause) is materialized as Iceberg under an Iceberg
    // SparkSessionCatalog, which treats a null provider as its own. For the Hive/parquet write
    // path (tableTypeString empty) with an explicit output location we therefore name a concrete
    // non-Iceberg provider so the catalog delegates the create to the session (Hive) catalog and
    // lands a real parquet table at that location. A provider plus LOCATION is already
    // external/unmanaged, so no EXTERNAL keyword is needed (and `EXTERNAL ... USING` is rejected
    // by Spark anyway, SPARK-30436). Without an output location we keep the original provider-less
    // managed create, preserving prior behavior for callers that don't set --output-location.
    val hasLocation = outputLocation.exists(_.trim.nonEmpty)
    val effectiveProvider =
      if (tableTypeString.nonEmpty) tableTypeString
      else if (hasLocation) "parquet"
      else ""
    val usingFragment = if (effectiveProvider.isEmpty) "" else s"USING $effectiveProvider"

    val noPartitions = StructType(
      schema
        .filterNot(field => partitionColumns.contains(field.name)))

    val createBody =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |    ${noPartitions.toDDL}
         |)""".stripMargin
    // Only append the USING line when a provider is set, so the managed path doesn't emit a
    // trailing blank line that the external path lacks.
    val createFragment = if (usingFragment.isEmpty) createBody else s"$createBody\n$usingFragment"

    val partitionFragment = if (partitionColumns != null && partitionColumns.nonEmpty) {

      val partitionDefinitions = schema
        .filter(field => partitionColumns.contains(field.name))
        .map(field => s"${field.name} ${field.dataType.catalogString}")

      s"""PARTITIONED BY (
         |    ${partitionDefinitions.mkString(",\n    ")}
         |)""".stripMargin

    } else {
      ""
    }

    val cloudPathLocation = if (outputLocation.exists(_.trim.nonEmpty)) {
      val location = outputLocation.get
      val cloudPath = if (location.endsWith("/")) location else location + "/"
      // The table's own name (the leaf segment of the identifier) becomes the location subdir.
      // Callers with a SparkSession should pass `tableLocationLeaf` (parsed via
      // Format.parseIdentifier) so dotted, backtick-quoted segments (`db`.`a.b`) resolve
      // correctly. The naive split fallback only handles simple db.table identifiers and is
      // used when no pre-parsed leaf is supplied (e.g. unit tests without a session).
      val finalTableName = tableLocationLeaf.getOrElse {
        // A dot inside a backtick-quoted segment can't be disambiguated from a namespace
        // separator by a naive split, so fail loudly instead of writing a silently wrong
        // LOCATION. Such callers must supply a pre-parsed tableLocationLeaf.
        require(
          !"`[^`]*`".r.findAllIn(tableName).exists(_.contains(".")),
          s"tableLocationLeaf must be supplied for '$tableName' (dot inside a quoted segment) when outputLocation is set"
        )
        val lastSegment = if (tableName.contains(".")) tableName.split("\\.").last else tableName
        lastSegment.stripPrefix("`").stripSuffix("`")
      }
      s"LOCATION '${escapeSqlStringValue(cloudPath + finalTableName)}/'"
    } else {
      ""
    }

    val propertiesFragment = if (tableProperties != null && tableProperties.nonEmpty) {
      s"""TBLPROPERTIES (
         |    ${(tableProperties + ("file_format" -> "PARQUET") + ("table_type" -> tableTypeString))
          .transform((k, v) => s"'${escapeSqlStringValue(k)}'='${escapeSqlStringValue(v)}'")
          .values
          .mkString(",\n   ")}
         |)""".stripMargin
    } else {
      ""
    }

    // Fragment order is significant: Spark SQL's CREATE TABLE grammar requires LOCATION to come
    // after PARTITIONED BY and before TBLPROPERTIES. Keep cloudPathLocation between the partition
    // and properties fragments if these are ever reordered.
    Seq(createFragment, partitionFragment, cloudPathLocation, propertiesFragment).mkString("\n")

  }

  // Needs provider
  def alterTablePropertiesSql(tableName: String, properties: Map[String, String]): String = {
    // Only SQL api exists for setting TBLPROPERTIES
    val propertiesString = properties
      .map { case (key, value) =>
        s"'${escapeSqlStringValue(key)}' = '${escapeSqlStringValue(value)}'"
      }
      .mkString(", ")
    s"ALTER TABLE $tableName SET TBLPROPERTIES ($propertiesString)"
  }

}
