package ai.chronon.spark.catalog

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreationUtilsTest extends AnyFlatSpec with Matchers {

  private val schema = StructType(
    Seq(
      StructField("key", StringType),
      StructField("value", IntegerType),
      StructField("ds", StringType)
    ))

  private val partitionColumns = List("ds")

  "createTableSql" should "emit a managed CREATE TABLE (no EXTERNAL/LOCATION) when no outputLocation is given" in {
    val sql = CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, "iceberg", None)

    sql should include("CREATE TABLE IF NOT EXISTS db.tbl")
    sql should not include "EXTERNAL"
    sql should not include "LOCATION"
    sql should include("USING iceberg")
    sql should include("PARTITIONED BY")
  }

  it should "emit USING parquet with a LOCATION for the Hive path so the catalog delegates off Iceberg" in {
    // The Hive write format uses an empty tableTypeString. A provider-less create would be
    // captured as Iceberg by SparkSessionCatalog, so with a location we name parquet explicitly.
    val sql =
      CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, "", Some("s3://bucket/warehouse"))

    sql should include("USING parquet")
    sql should not include "EXTERNAL"
    sql should include("LOCATION 's3://bucket/warehouse/tbl/'")
  }

  it should "NOT emit EXTERNAL with a USING provider (Spark rejects EXTERNAL+USING; SPARK-30436)" in {
    // Data-source tables (iceberg/delta/...) become external via LOCATION alone; EXTERNAL is illegal.
    val sql =
      CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, "iceberg", Some("s3://bucket/wh"))

    sql should not include "EXTERNAL"
    sql should include("USING iceberg")
    sql should include("LOCATION 's3://bucket/wh/tbl/'")
  }

  it should "not double the slash when outputLocation already ends with a slash" in {
    val sql =
      CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, "", Some("s3://bucket/warehouse/"))

    sql should include("LOCATION 's3://bucket/warehouse/tbl/'")
    sql should not include "warehouse//tbl"
  }

  it should "use only the last identifier segment (stripping backticks) for the table dir in LOCATION" in {
    val sql =
      CreationUtils.createTableSql("`db`.`tbl`", schema, partitionColumns, Map.empty, "", Some("s3://bucket/wh"))

    sql should include("LOCATION 's3://bucket/wh/tbl/'")
  }

  it should "use the pre-parsed leaf for the LOCATION subdir when one is supplied" in {
    // Format.createTable passes the leaf parsed via Format.parseIdentifier so dotted,
    // backtick-quoted segments (which the naive split would corrupt) resolve correctly.
    val sql =
      CreationUtils.createTableSql("`db`.`a.b`",
                                   schema,
                                   partitionColumns,
                                   Map.empty,
                                   "iceberg",
                                   Some("s3://bucket/wh"),
                                   tableLocationLeaf = Some("a.b"))

    sql should include("LOCATION 's3://bucket/wh/a.b/'")
  }

  it should "fail loudly when a dotted quoted identifier has no pre-parsed leaf and a location is set" in {
    // Without tableLocationLeaf the naive split can't tell a quoted dot from a namespace
    // separator, so it must refuse rather than emit a silently wrong LOCATION path.
    an[IllegalArgumentException] should be thrownBy
      CreationUtils.createTableSql("`db`.`a.b`", schema, partitionColumns, Map.empty, "iceberg", Some("s3://bucket/wh"))
  }

  it should "treat an empty-string outputLocation as managed (no EXTERNAL/LOCATION)" in {
    val sql = CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, "iceberg", Some(""))

    sql should not include "EXTERNAL"
    sql should not include "LOCATION"
  }

  it should "reject an invalid table type" in {
    an[IllegalArgumentException] should be thrownBy
      CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, "orc", None)
  }

  // Drive the provider matrix off the source-of-truth allow-list so a newly permitted table type
  // cannot silently escape coverage. Every provider must emit `USING <provider>`, never `EXTERNAL`
  // (SPARK-30436), and honor an explicit LOCATION when one is supplied.
  CreationUtils.ALLOWED_TABLE_TYPES.foreach { tableType =>
    it should s"emit a managed USING $tableType table (no EXTERNAL/LOCATION) without an outputLocation" in {
      val sql = CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, tableType, None)

      sql should include(s"USING $tableType")
      sql should not include "EXTERNAL"
      sql should not include "LOCATION"
      sql should include("PARTITIONED BY")
    }

    it should s"emit USING $tableType with a LOCATION (and never EXTERNAL) when an outputLocation is given" in {
      val sql =
        CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, tableType, Some("s3://bucket/wh"))

      sql should include(s"USING $tableType")
      sql should not include "EXTERNAL"
      sql should include("LOCATION 's3://bucket/wh/tbl/'")
    }
  }

  // The empty tableTypeString is the Hive default-format path. Without an output location it must
  // stay a provider-less managed CREATE (no USING) to preserve the original behavior for callers
  // that don't set --output-location; the USING-parquet-with-LOCATION variant is covered above.
  it should "emit a managed Hive CREATE TABLE (no USING/EXTERNAL/LOCATION) for empty type without a location" in {
    val sql = CreationUtils.createTableSql("db.tbl", schema, partitionColumns, Map.empty, "", None)

    sql should include("CREATE TABLE IF NOT EXISTS db.tbl")
    sql should not include "EXTERNAL"
    sql should not include "USING"
    sql should not include "LOCATION"
  }
}
