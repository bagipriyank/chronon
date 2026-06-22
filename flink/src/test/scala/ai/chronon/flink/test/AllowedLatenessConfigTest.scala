package ai.chronon.flink.test

import ai.chronon.flink.FlinkUtils
import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AllowedLatenessConfigTest extends AnyFlatSpec with Matchers {

  "allowed_lateness_seconds" should "be parsed from props" in {
    val props = Map("allowed_lateness_seconds" -> "60")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 60000L
  }

  it should "be parsed from topicInfo params" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map("allowed_lateness_seconds" -> "30"))

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 30000L
  }

  it should "prefer props over topicInfo" in {
    val props = Map("allowed_lateness_seconds" -> "60")
    val topicInfo = TopicInfo("test-topic", "kafka", Map("allowed_lateness_seconds" -> "30"))

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 60000L
  }

  it should "default to 0 when not configured" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "handle large values" in {
    val props = Map("allowed_lateness_seconds" -> "300")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 300000L
  }

  it should "ignore empty string values from props" in {
    val props = Map("allowed_lateness_seconds" -> "")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "ignore empty string values from topicInfo" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map("allowed_lateness_seconds" -> ""))

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "ignore whitespace-only values" in {
    val props = Map("allowed_lateness_seconds" -> "   ")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "trim whitespace from valid values" in {
    val props = Map("allowed_lateness_seconds" -> " 60 ")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 60000L
  }

  it should "clamp negative values to 0" in {
    val props = Map("allowed_lateness_seconds" -> "-60")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "throw IllegalArgumentException for non-numeric values" in {
    val props = Map("allowed_lateness_seconds" -> "abc")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val exception = the[IllegalArgumentException] thrownBy {
      FlinkUtils.getAllowedLatenessMs(props, topicInfo)
    }
    exception.getMessage should include("invalid allowed_lateness_seconds value")
  }

  it should "throw IllegalArgumentException for values that would overflow" in {
    val hugeValue = (Long.MaxValue / 1000 + 1).toString
    val props = Map("allowed_lateness_seconds" -> hugeValue)
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val exception = the[IllegalArgumentException] thrownBy {
      FlinkUtils.getAllowedLatenessMs(props, topicInfo)
    }
    exception.getMessage should include("exceeds maximum")
  }

  // defaultSeconds parameter tests

  it should "use defaultSeconds when property is absent" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    FlinkUtils.getAllowedLatenessMs(props, topicInfo, defaultSeconds = 172800L) shouldBe 172800000L
  }

  it should "use explicit property over defaultSeconds" in {
    val props = Map("allowed_lateness_seconds" -> "86400")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    FlinkUtils.getAllowedLatenessMs(props, topicInfo, defaultSeconds = 172800L) shouldBe 86400000L
  }

  it should "return 0 when defaultSeconds is 0 and property is absent" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    FlinkUtils.getAllowedLatenessMs(props, topicInfo, defaultSeconds = 0L) shouldBe 0L
  }

  it should "return 0 when defaultSeconds is negative" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    FlinkUtils.getAllowedLatenessMs(props, topicInfo, defaultSeconds = -10L) shouldBe 0L
  }

  it should "throw IllegalArgumentException when defaultSeconds would overflow" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    val hugeDefault = Long.MaxValue / 1000 + 1

    val exception = the[IllegalArgumentException] thrownBy {
      FlinkUtils.getAllowedLatenessMs(props, topicInfo, defaultSeconds = hugeDefault)
    }
    exception.getMessage should include("exceeds maximum")
  }
}
