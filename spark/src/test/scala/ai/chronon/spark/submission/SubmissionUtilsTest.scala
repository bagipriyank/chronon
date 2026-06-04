package ai.chronon.spark.submission

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SubmissionUtilsTest extends AnyFlatSpec with Matchers {

  "NodeConfReader.isCloudUri" should "detect supported schemes case-insensitively" in {
    Seq("GS://bucket/conf.json",
        "S3://bucket/conf.json",
        "S3A://bucket/conf.json",
        "ABFS://container@account.dfs.core.windows.net/conf.json",
        "ABFSS://container@account.dfs.core.windows.net/conf.json").foreach { uri =>
      NodeConfReader.isCloudUri(uri) shouldBe true
    }
  }

  it should "leave local paths as non-cloud URIs" in {
    NodeConfReader.isCloudUri("/tmp/conf.json") shouldBe false
    NodeConfReader.isCloudUri("file:///tmp/conf.json") shouldBe false
  }
}
