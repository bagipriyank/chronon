package ai.chronon.online.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{ArrayBlockingQueue, ThreadFactory, TimeUnit}

class InstrumentedThreadPoolExecutorTest extends AnyFlatSpec with Matchers {

  private class InspectableExecutor(context: Metrics.Context = InstrumentedThreadPoolExecutor.DefaultMetricsContext)
      extends InstrumentedThreadPoolExecutor(
        corePoolSize = 1,
        maximumPoolSize = 1,
        keepAliveTime = 1,
        unit = TimeUnit.SECONDS,
        workQueue = new ArrayBlockingQueue[Runnable](1),
        threadFactory = new ThreadFactory {
          override def newThread(r: Runnable): Thread = new Thread(r)
        },
        metricsContext = context
      ) {
    def exposedMetricsContext: Metrics.Context = metricsContext
  }

  it should "default threadpool metrics to fetcher" in {
    val executor = new InspectableExecutor()
    try {
      executor.exposedMetricsContext.environment shouldBe Metrics.Environment.Fetcher
      executor.exposedMetricsContext.suffix shouldBe "threadpool"
    } finally {
      executor.shutdownNow()
    }
  }

  it should "allow callers to provide threadpool metrics context" in {
    val executor = new InspectableExecutor(Metrics.Context(Metrics.Environment.Orchestrator).withSuffix("threadpool"))
    try {
      executor.exposedMetricsContext.environment shouldBe Metrics.Environment.Orchestrator
      executor.exposedMetricsContext.suffix shouldBe "threadpool"
    } finally {
      executor.shutdownNow()
    }
  }
}
