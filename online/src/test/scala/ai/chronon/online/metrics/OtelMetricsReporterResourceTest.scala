package ai.chronon.online.metrics

import io.opentelemetry.api.common.AttributeKey
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Verifies OtelMetricsReporter's resource attributes are deduplicated and honor OTEL_SERVICE_NAME. */
class OtelMetricsReporterResourceTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val ResourceKey = OtelMetricsReporter.MetricsExporterResourceKey
  private val ServiceNameKey = AttributeKey.stringKey("service.name")
  private val RegionKey = AttributeKey.stringKey("region")
  private val EnvKey = AttributeKey.stringKey("env")

  private var savedProp: Option[String] = None

  override def beforeEach(): Unit = {
    savedProp = Option(System.getProperty(ResourceKey))
    System.clearProperty(ResourceKey)
  }

  override def afterEach(): Unit = {
    savedProp match {
      case Some(v) => System.setProperty(ResourceKey, v)
      case None    => System.clearProperty(ResourceKey)
    }
  }

  "resource attribute parsing" should "not throw on duplicate keys in the system property" in {
    System.setProperty(ResourceKey, "service.name=first,region=us-east-1,service.name=second")

    val resource = OtelMetricsReporter.buildResource(_ => None)

    resource.getAttribute(ServiceNameKey) shouldBe "second"
    resource.getAttribute(RegionKey) shouldBe "us-east-1"
  }

  it should "take the last value when the same key appears multiple times" in {
    System.setProperty(ResourceKey, "env=staging,env=production")

    val resource = OtelMetricsReporter.buildResource(_ => None)

    resource.getAttribute(EnvKey) shouldBe "production"
  }

  "OTEL_RESOURCE_ATTRIBUTES" should "be included in the resource" in {
    System.clearProperty(ResourceKey)
    val envLookup: String => Option[String] = {
      case "OTEL_RESOURCE_ATTRIBUTES" => Some("region=us-east-1,team=ml")
      case _                          => None
    }

    val resource = OtelMetricsReporter.buildResource(envLookup)

    resource.getAttribute(RegionKey) shouldBe "us-east-1"
    resource.getAttribute(AttributeKey.stringKey("team")) shouldBe "ml"
  }

  it should "be overridden by the system property for the same key" in {
    System.setProperty(ResourceKey, "region=eu-west-1")
    val envLookup: String => Option[String] = {
      case "OTEL_RESOURCE_ATTRIBUTES" => Some("region=us-east-1")
      case _                          => None
    }

    val resource = OtelMetricsReporter.buildResource(envLookup)

    resource.getAttribute(RegionKey) shouldBe "eu-west-1"
  }

  "OTEL_SERVICE_NAME" should "be included in resource attributes when building the OTel client" in {
    System.clearProperty(ResourceKey)
    val envLookup: String => Option[String] = {
      case "OTEL_SERVICE_NAME" => Some("zipline-fetcher")
      case _                   => None
    }

    val resource = OtelMetricsReporter.buildResource(envLookup)

    resource.getAttribute(ServiceNameKey) shouldBe "zipline-fetcher"
  }

  it should "not set service.name when env var is absent" in {
    System.clearProperty(ResourceKey)
    val envLookup: String => Option[String] = _ => None

    val resource = OtelMetricsReporter.buildResource(envLookup)

    resource.getAttribute(ServiceNameKey) should not be "zipline-fetcher"
  }

  it should "win over service.name set in OTEL_RESOURCE_ATTRIBUTES" in {
    System.clearProperty(ResourceKey)
    val envLookup: String => Option[String] = {
      case "OTEL_RESOURCE_ATTRIBUTES" => Some("service.name=from-resource-attrs,region=us-east-1")
      case "OTEL_SERVICE_NAME"        => Some("from-service-name-env")
      case _                          => None
    }

    val resource = OtelMetricsReporter.buildResource(envLookup)

    resource.getAttribute(ServiceNameKey) shouldBe "from-service-name-env"
    resource.getAttribute(RegionKey) shouldBe "us-east-1"
  }

  "resource attribute parsing edge cases" should "keep values containing '=' (split on first '=' only)" in {
    System.setProperty(ResourceKey, "key=val=extra")

    val resource = OtelMetricsReporter.buildResource(_ => None)

    resource.getAttribute(AttributeKey.stringKey("key")) shouldBe "val=extra"
  }

  it should "reject entries with empty key or empty value" in {
    System.setProperty(ResourceKey, "=value,key=,valid=ok")

    val resource = OtelMetricsReporter.buildResource(_ => None)

    resource.getAttribute(AttributeKey.stringKey("")) shouldBe null
    resource.getAttribute(AttributeKey.stringKey("key")) shouldBe null
    resource.getAttribute(AttributeKey.stringKey("valid")) shouldBe "ok"
  }

}
