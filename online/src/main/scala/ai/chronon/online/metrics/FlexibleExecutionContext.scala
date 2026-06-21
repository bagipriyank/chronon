/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.metrics

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor

object FlexibleExecutionContext {
  private val instanceId = java.util.UUID.randomUUID().toString.take(8)

  // Create a thread factory so that we can name the threads for easier debugging
  val threadFactory: ThreadFactory = new ThreadFactory {
    private val counter = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setName(s"chronon-fetcher-$instanceId-${counter.incrementAndGet()}")
      // Set the context class loader if missing for libs like Spark (used in catalyst util) that rely on it to find classes
      if (t.getContextClassLoader == null) {
        t.setContextClassLoader(getClass.getClassLoader)
      }

      t
    }
  }

  def buildExecutor(metricsContext: Metrics.Context): ThreadPoolExecutor = {
    val cores = Runtime.getRuntime.availableProcessors()
    new InstrumentedThreadPoolExecutor(
      cores * 4, // corePoolSize
      cores * 4, // maxPoolSize
      600, // keepAliveTime
      TimeUnit.SECONDS, // keep alive time units
      new ArrayBlockingQueue[Runnable](10000),
      threadFactory,
      metricsContext = metricsContext
    )
  }

  lazy val buildExecutor: ThreadPoolExecutor =
    buildExecutor(InstrumentedThreadPoolExecutor.DefaultMetricsContext)

  def buildExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(buildExecutor)

  /** Creates a fresh executor for callers that need non-default threadpool metric ownership. */
  def buildExecutionContext(metricsContext: Metrics.Context): ExecutionContextExecutor =
    ExecutionContext.fromExecutor(buildExecutor(metricsContext))
}
