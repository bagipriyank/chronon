package ai.chronon.integrations.cloud_k8s

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{FlinkJob, SparkJob}
import io.fabric8.kubernetes.api.model.{
  ContainerStateBuilder,
  ContainerStatusBuilder,
  Pod,
  PodBuilder,
  PodListBuilder,
  PodStatusBuilder
}
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import org.junit.Assert.{assertEquals, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class K8sSubmitterTest extends AnyFlatSpec {

  private def submitter =
    new K8sSubmitter(
      sparkSubmitPath = "spark-submit",
      k8sMaster = "k8s://https://cluster.example:6443",
      namespace = "spark-jobs",
      serviceAccount = "spark-sa",
      image = "registry/spark:latest",
      fileUploadPath = "s3://warehouse/spark-staging",
      extraSparkConf = Map("spark.executor.memory" -> "4g")
    )

  "parseSemicolonSparkConf" should "parse semicolon-delimited conf and preserve commas in values" in {
    val parsed = K8sSubmitter.parseSemicolonSparkConf(
      "spark.executor.memory=4g;spark.jars=a.jar,b.jar;spark.driver.extraJavaOptions=-Dfoo=1,-Dbar=2"
    )
    assertEquals("4g", parsed("spark.executor.memory"))
    assertEquals("a.jar,b.jar", parsed("spark.jars"))
    assertEquals("-Dfoo=1,-Dbar=2", parsed("spark.driver.extraJavaOptions"))
  }

  it should "throw for malformed entries" in {
    val ex = intercept[IllegalArgumentException] {
      K8sSubmitter.parseSemicolonSparkConf("spark.executor.memory=4g;badtoken")
    }
    assertTrue(ex.getMessage.contains("Malformed token"))
  }

  "sanitizeSparkAppName" should "create a label-safe app id" in {
    val value = K8sSubmitter.sanitizeSparkAppName("Team/My GroupBy.v1")
    assertEquals("team-my-groupby-v1", value)
  }

  "getFilesArgs" should "parse --files and tolerate malformed flags" in {
    assertEquals(
      List("s3://a/1", "s3://a/2"),
      K8sSubmitter.getFilesArgs(Array("--files=s3://a/1,s3://a/2", "--other=x"))
    )
    assertEquals(Nil, K8sSubmitter.getFilesArgs(Array("--files")))
    assertEquals(Nil, K8sSubmitter.getFilesArgs(Array("--files=")))
  }

  it should "use only the first --files when several are present" in {
    assertEquals(
      List("first.txt"),
      K8sSubmitter.getFilesArgs(Array("--files=first.txt", "--files=ignored.txt"))
    )
  }

  // Empty env-getter so context-label tests can isolate the label-stamping behavior from the
  // ambient process env (CHRONON_DAG_ID etc. would otherwise leak through sys.env.get).
  private val noEnv: String => Option[String] = _ => None

  private val typedJobId = "spark.spark-jobs.my-gb.deadbeefcafe"

  "buildSparkSubmitArgv" should "include expected spark-submit args and wait flag" in {
    val argv = K8sSubmitter.buildSparkSubmitArgv(
      submitter = submitter,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
        MetadataName -> "my-gb",
        JobId -> "job-123"
      ),
      jobProperties = Map("spark.driver.memory" -> "2g"),
      files = List("s3://warehouse/confs/my-gb.v1"),
      rawArgs = Array("group-by-backfill", "--conf-path=my-gb.v1", "--local-conf-path=/tmp/my-gb.v1"),
      jobType = SparkJob,
      sparkAppName = "my-gb",
      chrononJobId = typedJobId,
      waitForAppCompletion = true,
      contextLabelEnv = noEnv
    )

    val command = argv.mkString(" ")
    assertTrue(command.contains("--master k8s://https://cluster.example:6443"))
    assertTrue(command.contains("--class ai.chronon.spark.Driver"))
    assertTrue(command.contains("spark.kubernetes.submission.waitAppCompletion=true"))
    assertTrue(command.contains("spark.kubernetes.driver.deleteOnTermination=false"))
    assertTrue(command.contains("--files s3://warehouse/confs/my-gb.v1"))
  }

  it should "stamp the typed chronon-job-id on the driver" in {
    val argv = K8sSubmitter.buildSparkSubmitArgv(
      submitter = submitter,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
        MetadataName -> "my-gb",
        JobId -> "job-123"
      ),
      jobProperties = Map.empty,
      files = Nil,
      rawArgs = Array("group-by-backfill"),
      jobType = SparkJob,
      sparkAppName = "my-gb",
      chrononJobId = typedJobId,
      waitForAppCompletion = false,
      contextLabelEnv = noEnv
    )
    val command = argv.mkString(" ")
    assertTrue(
      "driver pod must carry the typed chronon-job-id label",
      command.contains(s"spark.kubernetes.driver.label.chronon-job-id=$typedJobId")
    )
  }

  it should "pin app name, wait, and deleteOnTermination after user/job conf (last --conf wins)" in {
    val evil = new K8sSubmitter(
      sparkSubmitPath = "spark-submit",
      k8sMaster = "k8s://https://cluster.example:6443",
      namespace = "spark-jobs",
      serviceAccount = "spark-sa",
      image = "registry/spark:latest",
      fileUploadPath = "s3://warehouse/spark-staging",
      extraSparkConf = Map(
        "spark.app.name" -> "evil-name",
        "spark.kubernetes.submission.waitAppCompletion" -> "false",
        "spark.kubernetes.driver.deleteOnTermination" -> "true"
      )
    )
    val argv = K8sSubmitter.buildSparkSubmitArgv(
      submitter = evil,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
        MetadataName -> "my-gb",
        JobId -> "job-123"
      ),
      jobProperties = Map(
        "spark.app.name" -> "also-evil",
        "spark.kubernetes.submission.waitAppCompletion" -> "false"
      ),
      files = Nil,
      rawArgs = Array("group-by-backfill"),
      jobType = SparkJob,
      sparkAppName = "my-gb",
      chrononJobId = typedJobId,
      waitForAppCompletion = true,
      contextLabelEnv = noEnv
    )
    val confValues = argv
      .sliding(2, 1)
      .collect { case Seq("--conf", kv) => kv }
      .toSeq
    def lastValue(key: String): String =
      confValues.filter(_.startsWith(key + "=")).map(_.substring(key.length + 1)).last
    assertEquals("my-gb", lastValue("spark.app.name"))
    assertEquals("true", lastValue("spark.kubernetes.submission.waitAppCompletion"))
    assertEquals("false", lastValue("spark.kubernetes.driver.deleteOnTermination"))
  }

  it should "expand envVars into driver/executor env spark confs" in {
    // Exercise the trait helper + argv builder rather than submit(), since submit() forks spark-submit.
    val envVars = Map("MY_SECRET" -> "value1", "ANOTHER" -> "value2")
    val expanded = submitter.envVarsToSparkProperties(envVars)
    val argv = K8sSubmitter.buildSparkSubmitArgv(
      submitter = submitter,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
        MetadataName -> "my-gb",
        JobId -> "job-123"
      ),
      jobProperties = expanded,
      files = Nil,
      rawArgs = Array("group-by-backfill"),
      jobType = SparkJob,
      sparkAppName = "my-gb",
      chrononJobId = typedJobId,
      waitForAppCompletion = false,
      contextLabelEnv = noEnv
    )
    val command = argv.mkString(" ")
    assertTrue(command.contains("spark.kubernetes.driverEnv.MY_SECRET=value1"))
    assertTrue(command.contains("spark.executorEnv.MY_SECRET=value1"))
    assertTrue(command.contains("spark.kubernetes.driverEnv.ANOTHER=value2"))
    assertTrue(command.contains("spark.executorEnv.ANOTHER=value2"))
  }

  it should "stamp execution-context labels on driver and executor for set CHRONON_* env vars" in {
    val ctxEnv: String => Option[String] = Map(
      "CHRONON_DAG_ID" -> "chronon_group_bys_my_feature",
      "CHRONON_TASK_ID" -> "metadata-upload__chronon_group_bys_my_feature",
      "CHRONON_MODE" -> "metadata-upload",
      "CHRONON_DS" -> "2026-04-29",
      "CHRONON_TRY_NUMBER" -> "1",
      "CHRONON_RUN_ID" -> "manual__2026-04-29T07:00:00+00:00"
      // CHRONON_DS / TRY / RUN intentionally include hyphens / colons / pluses to exercise
      // sanitizeLabelValue's slug rewriting.
    ).get

    val argv = K8sSubmitter.buildSparkSubmitArgv(
      submitter = submitter,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
        MetadataName -> "my-gb",
        JobId -> "job-123"
      ),
      jobProperties = Map.empty,
      files = Nil,
      rawArgs = Array("group-by-backfill"),
      jobType = SparkJob,
      sparkAppName = "my-gb",
      chrononJobId = typedJobId,
      waitForAppCompletion = false,
      contextLabelEnv = ctxEnv
    )
    val command = argv.mkString(" ")
    val expected = Seq(
      "chronon-dag-id" -> "chronon_group_bys_my_feature",
      "chronon-task-id" -> "metadata-upload__chronon_group_bys_my_feature",
      "chronon-mode" -> "metadata-upload",
      "chronon-ds" -> "2026-04-29",
      "chronon-try-number" -> "1",
      "chronon-run-id" -> "manual__2026-04-29T07-00-00-00-00"
    )
    expected.foreach { case (key, value) =>
      assertTrue(s"driver should carry $key=$value", command.contains(s"spark.kubernetes.driver.label.$key=$value"))
      assertTrue(s"executor should carry $key=$value", command.contains(s"spark.kubernetes.executor.label.$key=$value"))
    }
  }

  it should "skip context labels whose CHRONON_* env vars are unset or blank" in {
    val ctxEnv: String => Option[String] = {
      case "CHRONON_DAG_ID" => Some("only-dag-id-set")
      case "CHRONON_DS"     => Some("   ") // blank should be filtered out
      case _                => None
    }
    val argv = K8sSubmitter.buildSparkSubmitArgv(
      submitter = submitter,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
        MetadataName -> "my-gb",
        JobId -> "job-123"
      ),
      jobProperties = Map.empty,
      files = Nil,
      rawArgs = Array("group-by-backfill"),
      jobType = SparkJob,
      sparkAppName = "my-gb",
      chrononJobId = typedJobId,
      waitForAppCompletion = false,
      contextLabelEnv = ctxEnv
    )
    val command = argv.mkString(" ")
    assertTrue(command.contains("spark.kubernetes.driver.label.chronon-dag-id=only-dag-id-set"))
    assertTrue(!command.contains("spark.kubernetes.driver.label.chronon-ds="))
    assertTrue(!command.contains("spark.kubernetes.driver.label.chronon-task-id="))
    assertTrue(!command.contains("spark.kubernetes.driver.label.chronon-mode="))
    assertTrue(!command.contains("spark.kubernetes.driver.label.chronon-try-number="))
    assertTrue(!command.contains("spark.kubernetes.driver.label.chronon-run-id="))
  }

  "mapPodPhaseToStatus" should "map pod phases into JobStatusType" in {
    val runningPod = new PodBuilder().withStatus(new PodStatusBuilder().withPhase("Running").build()).build()
    val failedPod = new PodBuilder().withStatus(new PodStatusBuilder().withPhase("Failed").build()).build()
    assertEquals(JobStatusType.RUNNING, K8sSubmitter.mapPodPhaseToStatus(runningPod))
    assertEquals(JobStatusType.FAILED, K8sSubmitter.mapPodPhaseToStatus(failedPod))
  }

  "readDriverTermination" should "extract driver exit code and reason" in {
    val terminatedState = new ContainerStateBuilder()
      .withNewTerminated()
      .withExitCode(137)
      .withReason("OOMKilled")
      .endTerminated()
      .build()

    val driverStatus = new ContainerStatusBuilder()
      .withName("spark-kubernetes-driver")
      .withState(terminatedState)
      .build()

    val pod = new PodBuilder()
      .withStatus(new PodStatusBuilder().withContainerStatuses(driverStatus).build())
      .build()

    assertEquals(Some((137, Some("OOMKilled"))), K8sSubmitter.readDriverTermination(pod))
  }

  "verifyDriverAndCleanup" should "fall back to spark-submit exit code when k8s lookup fails" in {
    val code = K8sSubmitter.verifyDriverAndCleanup(
      namespace = "spark-jobs",
      chrononJobId = typedJobId,
      sparkSubmitExitCode = 7,
      configResolver = () => throw new RuntimeException("k8s unavailable")
    )
    assertEquals(7, code)
  }

  private def podWithPhase(phase: String, container: Option[ContainerStatusBuilder] = None): Pod = {
    val statusBuilder = new PodStatusBuilder().withPhase(phase)
    container.foreach(c => statusBuilder.withContainerStatuses(c.build()))
    new PodBuilder().withStatus(statusBuilder.build()).build()
  }

  private def driverContainer(exitCode: Int, reason: String = null): ContainerStatusBuilder = {
    val terminatedBuilder = new ContainerStateBuilder().withNewTerminated().withExitCode(exitCode)
    val terminated =
      if (reason == null) terminatedBuilder.endTerminated().build()
      else terminatedBuilder.withReason(reason).endTerminated().build()
    new ContainerStatusBuilder().withName("spark-kubernetes-driver").withState(terminated)
  }

  "driverExitCodeFromPod" should "return 0 for Succeeded phase regardless of container state" in {
    assertEquals(Some(0), K8sSubmitter.driverExitCodeFromPod(podWithPhase("Succeeded")))
    assertEquals(Some(0), K8sSubmitter.driverExitCodeFromPod(podWithPhase("Succeeded", Some(driverContainer(0)))))
  }

  it should "return non-zero for Failed phase even when container exit code is 0 (false-success guard)" in {
    // Regression guard: spark-submit can report 0 for a driver whose pod is `Failed` while the
    // terminated container state has yet to populate. We must still surface failure.
    assertEquals(Some(1), K8sSubmitter.driverExitCodeFromPod(podWithPhase("Failed")))
    assertEquals(Some(1), K8sSubmitter.driverExitCodeFromPod(podWithPhase("Failed", Some(driverContainer(0)))))
  }

  it should "return the driver container exit code for Failed phase when populated" in {
    assertEquals(Some(137), K8sSubmitter.driverExitCodeFromPod(podWithPhase("Failed", Some(driverContainer(137)))))
    assertEquals(
      Some(137),
      K8sSubmitter.driverExitCodeFromPod(podWithPhase("Failed", Some(driverContainer(137, "OOMKilled"))))
    )
  }

  it should "defer (return None) for non-terminal phases without a non-zero terminated code" in {
    assertEquals(None, K8sSubmitter.driverExitCodeFromPod(podWithPhase("Running")))
    assertEquals(None, K8sSubmitter.driverExitCodeFromPod(podWithPhase("Pending")))
    assertEquals(None, K8sSubmitter.driverExitCodeFromPod(podWithPhase("Unknown")))
    assertEquals(None, K8sSubmitter.driverExitCodeFromPod(new PodBuilder().build()))
  }

  it should "still surface a non-zero terminated exit code under non-terminal phase (race window)" in {
    assertEquals(Some(2), K8sSubmitter.driverExitCodeFromPod(podWithPhase("Running", Some(driverContainer(2)))))
  }

  private def buildLabeledDriverPod(
      name: String,
      sparkAppName: String,
      chrononJobId: String,
      phase: String,
      exitCode: Int
  ): Pod = {
    new PodBuilder()
      .withNewMetadata()
      .withName(name)
      .withNamespace("spark-jobs")
      .addToLabels("spark-app-name", sparkAppName)
      .addToLabels("spark-app-selector", s"spark-${java.util.UUID.randomUUID().toString.replace("-", "")}")
      .addToLabels("spark-role", "driver")
      .addToLabels("chronon-job-id", chrononJobId)
      .endMetadata()
      .withStatus(
        new PodStatusBuilder()
          .withPhase(phase)
          .withContainerStatuses(driverContainer(exitCode).build())
          .build()
      )
      .build()
  }

  private def withMockServer[T](block: KubernetesMockServer => T): T = {
    val server = new KubernetesMockServer()
    server.start()
    try block(server)
    finally server.destroy()
  }

  // Driver-pod lookup URL for a given chronon-job-id under the spark-jobs namespace. We URL-
  // encode `=` and `,` only; dots / dashes / alphanumerics in the typed id pass through
  // un-escaped (they are unreserved chars per RFC 3986 / fabric8's URLEncoder usage).
  private def driverLookupPath(chrononJobId: String): String =
    s"/api/v1/namespaces/spark-jobs/pods?labelSelector=chronon-job-id%3D$chrononJobId%2Cspark-role%3Ddriver"

  "findDriverPod" should "look up pods by chronon-job-id AND spark-role=driver" in {
    withMockServer { server =>
      val pod = buildLabeledDriverPod("my-gb-abc-driver", "my-gb", typedJobId, "Failed", 1)
      server
        .expect()
        .get()
        .withPath(driverLookupPath(typedJobId))
        .andReturn(200, new PodListBuilder().withItems(List(pod).asJava).build())
        .always()

      val client = server.createClient()
      try {
        val found = K8sSubmitter.findDriverPod(client, "spark-jobs", typedJobId, attempts = 1, sleepMs = 0L)
        assertTrue("driver pod should be found via chronon-job-id label", found.isDefined)
        assertEquals("my-gb-abc-driver", found.get.getMetadata.getName)
      } finally client.close()
    }
  }

  it should "return None when no driver pod matches the label selector" in {
    val missingId = "spark.spark-jobs.missing.000000000000"
    withMockServer { server =>
      server
        .expect()
        .get()
        .withPath(driverLookupPath(missingId))
        .andReturn(200, new PodListBuilder().withItems(List.empty[Pod].asJava).build())
        .always()

      val client = server.createClient()
      try {
        assertEquals(None, K8sSubmitter.findDriverPod(client, "spark-jobs", missingId, attempts = 2, sleepMs = 0L))
      } finally client.close()
    }
  }

  it should "throw IllegalStateException when more than one pod carries the same chronon-job-id" in {
    withMockServer { server =>
      val pod1 = buildLabeledDriverPod("my-gb-abc-driver", "my-gb", typedJobId, "Succeeded", 0)
      val pod2 = buildLabeledDriverPod("my-gb-def-driver", "my-gb", typedJobId, "Failed", 1)
      server
        .expect()
        .get()
        .withPath(driverLookupPath(typedJobId))
        .andReturn(200, new PodListBuilder().withItems(List(pod1, pod2).asJava).build())
        .always()

      val client = server.createClient()
      try {
        val ex = intercept[IllegalStateException] {
          K8sSubmitter.findDriverPod(client, "spark-jobs", typedJobId, attempts = 1, sleepMs = 0L)
        }
        assertTrue(ex.getMessage.contains("Multiple driver pods"))
        assertTrue(ex.getMessage.contains("chronon-job-id"))
      } finally client.close()
    }
  }

  "verifyDriverAndCleanupWith" should
    "return a non-zero exit code when the driver pod is Failed even if spark-submit returned 0" in {
      withMockServer { server =>
        val pod = buildLabeledDriverPod("my-gb-abc-driver", "my-gb", typedJobId, "Failed", 1)
        server
          .expect()
          .get()
          .withPath(driverLookupPath(typedJobId))
          .andReturn(200, new PodListBuilder().withItems(List(pod).asJava).build())
          .always()
        server
          .expect()
          .delete()
          .withPath("/api/v1/namespaces/spark-jobs/pods/my-gb-abc-driver")
          .andReturn(200, pod)
          .once()

        val client = server.createClient()
        try {
          val code = K8sSubmitter.verifyDriverAndCleanupWith(
            client = client,
            namespace = "spark-jobs",
            chrononJobId = typedJobId,
            sparkSubmitExitCode = 0
          )
          assertEquals(1, code)
        } finally client.close()
      }
    }

  it should "return 0 when the driver pod is Succeeded even if spark-submit reported a non-zero code" in {
    withMockServer { server =>
      val pod = buildLabeledDriverPod("my-gb-abc-driver", "my-gb", typedJobId, "Succeeded", 0)
      server
        .expect()
        .get()
        .withPath(driverLookupPath(typedJobId))
        .andReturn(200, new PodListBuilder().withItems(List(pod).asJava).build())
        .always()
      server
        .expect()
        .delete()
        .withPath("/api/v1/namespaces/spark-jobs/pods/my-gb-abc-driver")
        .andReturn(200, pod)
        .once()

      val client = server.createClient()
      try {
        val code = K8sSubmitter.verifyDriverAndCleanupWith(
          client = client,
          namespace = "spark-jobs",
          chrononJobId = typedJobId,
          sparkSubmitExitCode = 42
        )
        assertEquals(0, code)
      } finally client.close()
    }
  }

  it should "isolate by chronon-job-id so a stale Succeeded sibling doesn't mask this run" in {
    // Regression guard for the false-success bug: pre-fix, lookup-by-spark-app-name would pick
    // the leftover Succeeded pod from yesterday's run and report SUCCESS for today's Failed
    // driver. Now the lookup is by chronon-job-id so only this submission's pod is returned.
    withMockServer { server =>
      val freshFailedPod = buildLabeledDriverPod("my-gb-fresh-driver", "my-gb", typedJobId, "Failed", 7)
      // Mock server filters by labelSelector; only the freshly-labeled pod comes back even
      // though a stale Succeeded sibling shares spark-app-name=my-gb in the namespace.
      server
        .expect()
        .get()
        .withPath(driverLookupPath(typedJobId))
        .andReturn(200, new PodListBuilder().withItems(List(freshFailedPod).asJava).build())
        .always()
      server
        .expect()
        .delete()
        .withPath("/api/v1/namespaces/spark-jobs/pods/my-gb-fresh-driver")
        .andReturn(200, freshFailedPod)
        .once()

      val client = server.createClient()
      try {
        val code = K8sSubmitter.verifyDriverAndCleanupWith(
          client = client,
          namespace = "spark-jobs",
          chrononJobId = typedJobId,
          sparkSubmitExitCode = 0 // spark-submit erroneously reports success
        )
        assertEquals(7, code)
      } finally client.close()
    }
  }

  it should "fall back to spark-submit exit code when the driver pod is not observable" in {
    val nopeId = "spark.spark-jobs.nope.000000000000"
    withMockServer { server =>
      server
        .expect()
        .get()
        .withPath(driverLookupPath(nopeId))
        .andReturn(200, new PodListBuilder().withItems(List.empty[Pod].asJava).build())
        .always()

      val client = server.createClient()
      try {
        val code = K8sSubmitter.verifyDriverAndCleanupWith(
          client = client,
          namespace = "spark-jobs",
          chrononJobId = nopeId,
          sparkSubmitExitCode = 5,
          attempts = 1,
          sleepMs = 0L
        )
        assertEquals(5, code)
      } finally client.close()
    }
  }

  // K8s label-value regex (api/.../labels/v1/api.go). Compiled as a String so we can use
  // java.lang.String.matches under both 2.12 and 2.13 (Scala 2.12 Regex has no `.matches`).
  private val LabelValueRegex = "^[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$"

  "formatChrononJobId" should "produce K8s-label-safe ids of the form spark.<ns>.<app>.<uuid12>" in {
    val id = K8sSubmitter.formatChrononJobId(
      jobType = SparkJob,
      namespace = "spark-jobs",
      sparkAppName = "my-gb",
      jobIdUuid = "12345678-90ab-cdef-1234-567890abcdef"
    )
    assertTrue(s"id '$id' must start with spark.", id.startsWith("spark."))
    assertTrue(s"id '$id' must end with uuid12 (12 hex)", id.endsWith(".1234567890ab"))
    assertTrue(s"id '$id' must include the namespace slug", id.contains(".spark-jobs."))
    assertTrue(s"id '$id' must include the app slug", id.contains(".my-gb."))
    assertTrue(s"id '$id' must satisfy K8s label-value regex", id.matches(LabelValueRegex))
  }

  it should "fit in 63 chars even with a long namespace and app name" in {
    val longNs = "really-long-namespace-name-that-exceeds-the-cap"
    val longApp = "very-long-app-name-from-a-deeply-nested-team-and-conf-id"
    val id = K8sSubmitter.formatChrononJobId(
      jobType = SparkJob,
      namespace = longNs,
      sparkAppName = longApp,
      jobIdUuid = "deadbeef-cafe-babe-1234-567890abcdef"
    )
    assertTrue(s"id '$id' length=${id.length} must be <= 63", id.length <= 63)
    assertTrue(s"id '$id' must satisfy K8s label-value regex", id.matches(LabelValueRegex))
    assertTrue("uuid12 slot must be preserved verbatim", id.endsWith(".deadbeefcafe"))
  }

  it should "use a different prefix for FlinkJob (forward-compat scaffold)" in {
    val id = K8sSubmitter.formatChrononJobId(
      jobType = FlinkJob,
      namespace = "ns",
      sparkAppName = "app",
      jobIdUuid = "00000000-0000-0000-0000-000000000001"
    )
    assertTrue(id.startsWith("flink."))
  }

  "sanitizeLabelValue" should "rewrite forbidden chars, trim, and cap at 63" in {
    assertEquals("", K8sSubmitter.sanitizeLabelValue(""))
    assertEquals("", K8sSubmitter.sanitizeLabelValue(null))
    assertEquals("", K8sSubmitter.sanitizeLabelValue("---___"))
    assertEquals("a.b-c", K8sSubmitter.sanitizeLabelValue("-a.b::c-"))
    val long = K8sSubmitter.sanitizeLabelValue("a" * 100)
    assertEquals(63, long.length)
    assertTrue(long.matches(LabelValueRegex))
    val truncated = K8sSubmitter.sanitizeLabelValue("x" * 60 + "-aaa")
    assertTrue(truncated.matches(LabelValueRegex))
  }

  "resolveSparkAppName" should "honor CHRONON_SPARK_APP_NAME over MetadataName / args / jobId" in {
    val envOverride: String => Option[String] = {
      case "CHRONON_SPARK_APP_NAME" => Some("metadata-upload__chronon_group_bys_my_feature")
      case _                        => None
    }
    val name = K8sSubmitter.resolveSparkAppName(
      submissionProperties = Map(MetadataName -> "should-be-ignored", JobId -> "fallback"),
      args = Array("group-by-backfill"),
      envLookup = envOverride
    )
    assertEquals("metadata-upload-chronon-group-bys-my-feature", name)
  }

  it should "fall through to MetadataName when the env var is unset" in {
    val name = K8sSubmitter.resolveSparkAppName(
      submissionProperties = Map(MetadataName -> "my-gb", JobId -> "fallback"),
      args = Array("group-by-backfill"),
      envLookup = _ => None
    )
    assertEquals("my-gb", name)
  }

  "SubmissionMode.parse" should "accept the canonical mode strings" in {
    assertEquals(SubmissionMode.SparkSubmit, SubmissionMode.parse(""))
    assertEquals(SubmissionMode.SparkSubmit, SubmissionMode.parse("spark-submit"))
    assertEquals(SubmissionMode.SparkSubmit, SubmissionMode.parse("  Spark-Submit  "))
    assertEquals(SubmissionMode.SparkOperator, SubmissionMode.parse("spark-operator"))
    assertEquals(SubmissionMode.SparkOperator, SubmissionMode.parse("SPARK-OPERATOR"))
  }

  it should "throw for unknown mode strings" in {
    val ex = intercept[IllegalArgumentException] { SubmissionMode.parse("dataproc") }
    assertTrue(ex.getMessage.contains("dataproc"))
    assertTrue(ex.getMessage.contains(SubmissionMode.EnvVar))
  }

  "SubmissionMode.resolve" should "prefer submission-properties over job properties over default" in {
    assertEquals(
      SubmissionMode.SparkOperator,
      SubmissionMode.resolve(
        Map(SubmissionMode.PropertyKey -> "spark-operator"),
        Map(SubmissionMode.PropertyKey -> "spark-submit")
      )
    )
    assertEquals(
      SubmissionMode.SparkOperator,
      SubmissionMode.resolve(Map.empty, Map(SubmissionMode.PropertyKey -> "spark-operator"))
    )
    assertEquals(SubmissionMode.Default, SubmissionMode.resolve(Map.empty, Map.empty))
    assertEquals(
      SubmissionMode.SparkOperator,
      SubmissionMode.resolve(Map(SubmissionMode.PropertyKey -> "spark-operator"))
    )
    assertEquals(SubmissionMode.Default, SubmissionMode.resolve(Map.empty))
  }

  "buildSparkSubmitArgv" should "omit submission-mode from spark conf" in {
    val argv = K8sSubmitter.buildSparkSubmitArgv(
      submitter = submitter,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
        JobId -> "job-id",
        MetadataName -> "gb",
        ZiplineVersion -> "1.0"
      ),
      jobProperties = Map(
        SubmissionMode.PropertyKey -> "spark-submit",
        "spark.executor.memory" -> "8g"
      ),
      files = Nil,
      rawArgs = Array("group-by-backfill"),
      jobType = SparkJob,
      sparkAppName = "gb",
      chrononJobId = "spark:spark-jobs:gb:job-id",
      waitForAppCompletion = false
    )
    val confTokens = argv.zip(argv.tail).collect { case ("--conf", kv) => kv }
    assertTrue(confTokens.exists(_.startsWith("spark.executor.memory=")))
    assertTrue(!confTokens.exists(_.startsWith(s"${SubmissionMode.PropertyKey}=")))
  }

  "K8sSubmitter.submit" should "reject spark-operator via CRD ingress stub" in {
    val ex = intercept[UnsupportedOperationException] {
      submitter.submit(
        jobType = SparkJob,
        submissionProperties = Map(
          MainClass -> "ai.chronon.spark.Driver",
          JarURI -> "s3://artifacts/cloud_k8s_lib_deploy.jar",
          JobId -> "job-uuid",
          MetadataName -> "test-gb",
          ZiplineVersion -> "1.0"
        ),
        jobProperties = Map(SubmissionMode.PropertyKey -> "spark-operator"),
        files = Nil,
        labels = Map.empty,
        envVars = Map.empty
      )
    }
    assertTrue(ex.getMessage.contains("spark-operator"))
    assertTrue(ex.getMessage.contains("not implemented"))
  }
}
