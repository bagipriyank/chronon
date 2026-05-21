package ai.chronon.integrations.cloud_k8s

import ai.chronon.api.Builders.MetaData
import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{FlinkJob, JobSubmitter, JobType, SparkJob}
import io.fabric8.kubernetes.api.model.{ContainerStatus, Pod}
import io.fabric8.kubernetes.client.{Config, KubernetesClient, KubernetesClientBuilder}

import java.io.IOException
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Kubernetes job submitter for Chronon (Spark today; Flink-on-K8s planned).
  * Named generically (`K8sSubmitter`, not `K8sSparkSubmitter`) to leave room
  * for Flink without a rename.
  *
  * Submission mode is resolved from job/mode-config, submission properties, and
  * `CHRONON_K8S_SUBMISSION_MODE` (see [[SubmissionMode]]). `SparkSubmit` shells out
  * to `spark-submit`; `SparkOperator` dispatches to companion CRD ingress stubs until
  * SparkApplication apply is implemented.
  */
class K8sSubmitter(
    val sparkSubmitPath: String,
    val k8sMaster: String,
    val namespace: String,
    val serviceAccount: String,
    val image: String,
    val fileUploadPath: String,
    val extraSparkConf: Map[String, String],
    val configResolver: () => Config = () => Config.autoConfigure(null)
) extends JobSubmitter {

  override def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = false

  override def submit(
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      envVars: Map[String, String],
      args: String*
  ): String = {
    require(jobType == SparkJob, "K8sSubmitter only supports Spark batch jobs")
    if (labels.nonEmpty) {
      logger.warn(
        "K8sSubmitter.submit ignores `labels` (not mapped to spark.kubernetes.*.label.*); use SPARK_K8S_CONF or job properties if you need driver/executor labels."
      )
    }
    val rawArgs = args.toArray
    val appName = K8sSubmitter.resolveSparkAppName(submissionProperties, rawArgs)
    val jobIdUuid = submissionProperties.getOrElse(
      JobId,
      throw new IllegalArgumentException(s"Missing required submission property: $JobId")
    )
    val chrononJobId = K8sSubmitter.formatChrononJobId(jobType, namespace, appName, jobIdUuid)
    val effectiveJobProperties = jobProperties ++ envVarsToSparkProperties(envVars)
    SubmissionMode.resolve(submissionProperties, effectiveJobProperties) match {
      case SubmissionMode.SparkOperator =>
        K8sSubmitter.submitViaSparkApplicationCrd(
          submitter = this,
          jobType = jobType,
          submissionProperties = submissionProperties,
          jobProperties = effectiveJobProperties,
          files = files,
          labels = labels,
          envVars = envVars,
          rawArgs = rawArgs,
          appName = appName,
          chrononJobId = chrononJobId
        )
      case SubmissionMode.SparkSubmit =>
        val argv = K8sSubmitter.buildSparkSubmitArgv(
          submitter = this,
          submissionProperties = submissionProperties,
          jobProperties = effectiveJobProperties,
          files = files,
          rawArgs = rawArgs,
          jobType = jobType,
          sparkAppName = appName,
          chrononJobId = chrononJobId,
          waitForAppCompletion = false
        )
        val proc = K8sSubmitter.runSparkSubmitProcess(argv, inheritIo = true)
        val exit = K8sSubmitter.waitForSparkSubmitExitCode(proc)
        if (exit != 0) throw new RuntimeException(s"spark-submit failed with exit code $exit")
        chrononJobId
    }
  }

  override def status(jobId: String): JobStatusType = {
    var client: KubernetesClient = null
    try {
      client = new KubernetesClientBuilder().withConfig(configResolver()).build()
      K8sSubmitter.findDriverPod(client, namespace, jobId, attempts = 1, sleepMs = 0L) match {
        case None =>
          logger.warn(
            s"No driver pod found for ${K8sSubmitter.ChrononJobIdLabel}=$jobId," +
              s"${K8sSubmitter.SparkRoleLabel}=${K8sSubmitter.SparkRoleDriver} in namespace $namespace"
          )
          JobStatusType.UNKNOWN
        case Some(pod) =>
          K8sSubmitter.mapPodPhaseToStatus(pod)
      }
    } catch {
      case e: io.fabric8.kubernetes.client.KubernetesClientException if e.getCode == 403 =>
        logger.error(
          "K8s API returned 403 querying driver pod. Ensure the submitter's service account has get/list on pods."
        )
        throw e
      case NonFatal(e) =>
        logger.error(
          s"K8s driver status query failed (${e.getClass.getSimpleName}): ${Option(e.getMessage).getOrElse("")}",
          e
        )
        throw e
    } finally {
      if (client != null) client.close()
    }
  }

  override def kill(jobId: String): Unit = {
    var client: KubernetesClient = null
    try {
      client = new KubernetesClientBuilder().withConfig(configResolver()).build()
      client
        .pods()
        .inNamespace(namespace)
        .withLabel(K8sSubmitter.ChrononJobIdLabel, jobId)
        .delete()
    } finally {
      if (client != null) client.close()
    }
  }
}

object K8sSubmitter {

  // Spark stamps `spark-app-name` (from `spark.app.name`) and `spark-role=driver` on every driver
  // pod. We treat `spark-app-name` as a human-readable display label only -- it is NOT unique per
  // submission (same conf with different modes/runs/retries produces the same value), so basing
  // pod lookup on it can pick a stale `Succeeded` sibling and report false success. Authoritative
  // pod identification uses our own `chronon-job-id` label which carries the typed job id
  // produced by `formatChrononJobId` (always unique per submission).
  private[cloud_k8s] val SparkAppNameLabel = "spark-app-name"
  private[cloud_k8s] val SparkRoleLabel = "spark-role"
  private[cloud_k8s] val SparkRoleDriver = "driver"
  private[cloud_k8s] val ChrononJobIdLabel = "chronon-job-id"

  /** When set, this env var overrides every other source of `spark.app.name`.
    *
    * Provided for orchestrators (e.g. Airflow) that want to inject a human-readable display name
    * (typically the task id) without having to touch the CLI args of every wrapped runner.
    * Lookup correctness no longer depends on the value being unique because pod lookup uses the
    * separate `chronon-job-id` label.
    */
  private[cloud_k8s] val SparkAppNameEnvVar = "CHRONON_SPARK_APP_NAME"

  /** Env var -> K8s label key mapping for execution-context labels.
    *
    * Each entry expands to `spark.kubernetes.{driver,executor}.label.<labelKey> = <env value>`
    * when the env var is set and non-empty. Stamping is skipped silently when the env var is
    * missing so non-orchestrated callers degrade gracefully.
    *
    * Label keys are `chronon-` prefixed so they remain unambiguous when the namespace is shared
    * with other workloads that may stamp their own `task` / `workflow` / etc. labels.
    */
  private[cloud_k8s] val ContextLabelEnvVars: Seq[(String, String)] = Seq(
    "CHRONON_DAG_ID" -> "chronon-dag-id",
    "CHRONON_TASK_ID" -> "chronon-task-id",
    "CHRONON_MODE" -> "chronon-mode",
    "CHRONON_DS" -> "chronon-ds",
    "CHRONON_TRY_NUMBER" -> "chronon-try-number",
    "CHRONON_RUN_ID" -> "chronon-run-id"
  )

  private val SparkSubmitMaxWaitDays: Long = 7L

  // Bounded retry while the API server catches up with the driver pod that spark-submit just landed.
  private[cloud_k8s] val VerifyDriverPodAttempts: Int = 5
  private[cloud_k8s] val VerifyDriverPodSleepMs: Long = 2000L

  private[cloud_k8s] def waitForSparkSubmitExitCode(proc: java.lang.Process): Int = {
    val completed = proc.waitFor(SparkSubmitMaxWaitDays, TimeUnit.DAYS)
    if (!completed) {
      proc.destroyForcibly()
      throw new RuntimeException("spark-submit did not complete within the wait window")
    }
    proc.exitValue()
  }

  def parseSemicolonSparkConf(raw: String): Map[String, String] = {
    if (raw == null || raw.trim.isEmpty) return Map.empty
    val out = mutable.Map.empty[String, String]
    raw.split(';').foreach { token =>
      val t = token.trim
      if (t.nonEmpty) {
        val idx = t.indexOf('=')
        if (idx <= 0) {
          throw new IllegalArgumentException(s"Malformed token in SPARK_K8S_CONF: '$t' (expected key=value)")
        }
        val k = t.substring(0, idx).trim
        if (k.isEmpty) throw new IllegalArgumentException(s"Empty key in SPARK_K8S_CONF token: '$t'")
        val v = t.substring(idx + 1).trim
        out += k -> v
      }
    }
    out.toMap
  }

  def getFilesArgs(args: Array[String]): List[String] = {
    val filesArgs = args.filter(_.startsWith(FilesArgKeyword))
    if (filesArgs.length > 1) {
      JobSubmitter.logger.warn(
        s"Multiple ${FilesArgKeyword} arguments (${filesArgs.length}); only the first is used for staged config paths"
      )
    }
    if (filesArgs.isEmpty) Nil
    else {
      val head = filesArgs.head
      val eqIdx = head.indexOf('=')
      if (eqIdx < 0 || eqIdx >= head.length - 1) Nil
      else
        head.substring(eqIdx + 1).split(",").map(_.trim).filter(_.nonEmpty).toList
    }
  }

  def createSubmissionPropsMap(args: Array[String]): Map[String, String] = {
    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new IllegalArgumentException("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new IllegalArgumentException("Missing required argument: " + MainClassKeyword))
    val jobId = JobSubmitter
      .getArgValue(args, JobIdArgKeyword)
      .getOrElse(throw new IllegalArgumentException("Missing required argument: " + JobIdArgKeyword))
    val metadataName = Option(JobSubmitter.getMetadata(args).getOrElse(MetaData()).getName).getOrElse("")
    val ziplineVersion = JobSubmitter
      .getArgValue(args, ZiplineVersionArgKeyword)
      .getOrElse(throw new IllegalArgumentException("Missing required argument: " + ZiplineVersionArgKeyword))
    val additional = JobSubmitter.getArgValue(args, AdditionalJarsUriArgKeyword).map(AdditionalJars -> _).toMap
    Map(
      MainClass -> mainClass,
      JarURI -> jarUri,
      MetadataName -> metadataName,
      JobId -> jobId,
      ZiplineVersion -> ziplineVersion
    ) ++ additional
  }

  def sanitizeSparkAppName(raw: String): String = {
    val slug = raw.toLowerCase.replaceAll("[^a-z0-9\\-]+", "-").replaceAll("-+", "-")
    val trimmed = slug.stripPrefix("-").stripSuffix("-")
    val base = if (trimmed.isEmpty) "chronon-app" else trimmed
    val maxLen = 63
    if (base.length <= maxLen) base else base.take(maxLen).stripSuffix("-")
  }

  /** Render `raw` into a K8s label-value-safe string per the spec
    * `[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?`: keep only `[A-Za-z0-9._-]`, collapse runs
    * of `-`, trim non-alphanumerics from both ends, cap at 63 chars (re-trimming after).
    *
    * Returns the empty string when no alphanumeric content survives; callers must skip empty
    * results (a label value of `""` is rejected by K8s).
    */
  private[cloud_k8s] def sanitizeLabelValue(raw: String): String = {
    if (raw == null) return ""
    val slug = raw.replaceAll("[^A-Za-z0-9._-]", "-").replaceAll("-+", "-")
    val trimmed = slug.dropWhile(c => !c.isLetterOrDigit).reverse.dropWhile(c => !c.isLetterOrDigit).reverse
    if (trimmed.length <= 63) trimmed
    else trimmed.take(63).reverse.dropWhile(c => !c.isLetterOrDigit).reverse
  }

  /** Build a typed, K8s-label-safe job id of the form
    * `spark.<namespace>.<sanitized-app-name>.<uuid12>`.
    *
    * Mirrors `EmrServerlessSubmitter`'s `<jobType>:...` shape *structurally* but uses dots rather
    * than colons because K8s label values reject `:` (regex above). The 12-hex-char UUID prefix
    * gives 2^48 collision-free ids per (namespace, app-name) tuple, which is far more than any
    * realistic single-tenant pod fleet.
    *
    * Total length is bounded at K8s' 63-char label-value limit; namespace and app-name are
    * truncated proportionally if needed (the UUID slot is preserved verbatim because it carries
    * the uniqueness guarantee).
    */
  private[cloud_k8s] def formatChrononJobId(
      jobType: JobType,
      namespace: String,
      sparkAppName: String,
      jobIdUuid: String
  ): String = {
    // Only Spark today. Structure parametrized so the future Flink-on-K8s submitter can reuse
    // this with `prefix = "flink"` (matching EmrServerlessSubmitter's per-jobType prefix).
    val prefix = jobType match {
      case SparkJob => "spark"
      case FlinkJob => "flink"
    }
    val nsSlug = sanitizeLabelValue(namespace)
    val appSlug = sanitizeLabelValue(sparkAppName)
    val shortUuid = jobIdUuid.replace("-", "").toLowerCase.take(12)
    // K8s label values: <= 63 chars. Reserve fixed pieces (prefix + 3 dots + uuid12) and split
    // the remaining budget between namespace (capped at 20) and app name.
    val MaxLen = 63
    val fixedLen = prefix.length + 3 + shortUuid.length
    val varBudget = math.max(0, MaxLen - fixedLen)
    val nsMax = math.min(nsSlug.length, math.min(20, varBudget))
    val ns = nsSlug.take(nsMax)
    val appBudget = math.max(0, varBudget - ns.length)
    val app = appSlug.take(appBudget)
    sanitizeLabelValue(s"$prefix.$ns.$app.$shortUuid")
  }

  def resolveSparkAppName(
      submissionProperties: Map[String, String],
      args: Array[String],
      envLookup: String => Option[String] = sys.env.get
  ): String = {
    val fromEnv = envLookup(SparkAppNameEnvVar).map(_.trim).filter(_.nonEmpty)
    val fromProps = submissionProperties.get(MetadataName).map(_.trim).filter(_.nonEmpty)
    val fromMeta = JobSubmitter.getMetadata(args).flatMap(m => Option(m.getName).map(_.trim).filter(_.nonEmpty))
    val fromJobId = submissionProperties.get(JobId).map(_.trim).filter(_.nonEmpty)
    sanitizeSparkAppName(
      fromEnv.orElse(fromProps).orElse(fromMeta).orElse(fromJobId).getOrElse("chronon-app")
    )
  }

  def fromEnv(configResolver: () => Config = () => Config.autoConfigure(null)): K8sSubmitter = {
    val master = sys.env.getOrElse(
      "SPARK_K8S_MASTER",
      throw new IllegalStateException("Required environment variable SPARK_K8S_MASTER is not set")
    )
    val image = sys.env.getOrElse(
      "SPARK_K8S_IMAGE",
      throw new IllegalStateException("Required environment variable SPARK_K8S_IMAGE is not set")
    )
    val uploadPath = sys.env.getOrElse(
      "SPARK_K8S_FILE_UPLOAD_PATH",
      throw new IllegalStateException("Required environment variable SPARK_K8S_FILE_UPLOAD_PATH is not set")
    )
    new K8sSubmitter(
      sparkSubmitPath = sys.env.getOrElse("SPARK_SUBMIT_PATH", "spark-submit"),
      k8sMaster = master,
      namespace = sys.env.getOrElse("SPARK_K8S_NAMESPACE", "default"),
      serviceAccount = sys.env.getOrElse("SPARK_K8S_SERVICE_ACCOUNT", "default"),
      image = image,
      fileUploadPath = uploadPath,
      extraSparkConf = parseSemicolonSparkConf(sys.env.getOrElse("SPARK_K8S_CONF", "")),
      configResolver = configResolver
    )
  }

  def buildSparkSubmitArgv(
      submitter: K8sSubmitter,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      rawArgs: Array[String],
      jobType: JobType,
      sparkAppName: String,
      chrononJobId: String,
      waitForAppCompletion: Boolean,
      contextLabelEnv: String => Option[String] = sys.env.get
  ): Seq[String] = {
    val mainClass =
      submissionProperties.getOrElse(MainClass, throw new IllegalArgumentException("Main class not found"))
    val jarUri = submissionProperties.getOrElse(JarURI, throw new IllegalArgumentException("Jar URI not found"))
    val appArgs = JobSubmitter.getApplicationArgs(jobType, rawArgs)

    val confPairs = mutable.ArrayBuffer[(String, String)](
      "spark.kubernetes.namespace" -> submitter.namespace,
      "spark.kubernetes.authenticate.driver.serviceAccountName" -> submitter.serviceAccount,
      "spark.kubernetes.container.image" -> submitter.image,
      "spark.kubernetes.file.upload.path" -> submitter.fileUploadPath
    )
    submitter.extraSparkConf.foreach { case (k, v) => confPairs += k -> v }
    // Chronon-only keys (e.g. submission-mode) must not become spark-submit --conf entries.
    jobProperties.foreach {
      case (k, v) if k != SubmissionMode.PropertyKey => confPairs += k -> v
      case _                                         =>
    }
    // Spark uses last --conf value per key; append so submitter-controlled settings cannot be
    // overridden by SPARK_K8S_CONF or mode job properties (wait/delete/app name/labels).
    confPairs += "spark.app.name" -> sparkAppName
    confPairs += "spark.kubernetes.submission.waitAppCompletion" -> waitForAppCompletion.toString
    confPairs += "spark.kubernetes.driver.deleteOnTermination" -> "false"
    val sanitizedJobId = sanitizeLabelValue(chrononJobId)
    if (sanitizedJobId.nonEmpty) {
      confPairs += s"spark.kubernetes.driver.label.$ChrononJobIdLabel" -> sanitizedJobId
    }
    // Mirror the env-driven context labels onto both driver and executor pods so ops queries
    // like `kubectl get pods -l chronon-mode=metadata-upload,chronon-ds=2026-04-29` return
    // executors alongside the driver. Skip silently when the corresponding env var is unset.
    ContextLabelEnvVars.foreach { case (envName, labelKey) =>
      contextLabelEnv(envName).map(_.trim).filter(_.nonEmpty).foreach { raw =>
        val value = sanitizeLabelValue(raw)
        if (value.nonEmpty) {
          confPairs += s"spark.kubernetes.driver.label.$labelKey" -> value
          confPairs += s"spark.kubernetes.executor.label.$labelKey" -> value
        }
      }
    }

    val argv = mutable.ArrayBuffer[String](
      submitter.sparkSubmitPath,
      "--master",
      submitter.k8sMaster,
      "--deploy-mode",
      "cluster",
      "--class",
      mainClass
    )
    confPairs.foreach { case (k, v) =>
      argv += "--conf"
      argv += s"$k=$v"
    }
    if (files.nonEmpty) {
      argv += "--files"
      argv += files.mkString(",")
    }
    submissionProperties.get(AdditionalJars).foreach { jars =>
      argv += "--jars"
      argv += jars
    }
    argv += jarUri
    appArgs.foreach(argv += _)
    argv.toSeq
  }

  def runSparkSubmitProcess(argv: Seq[String], inheritIo: Boolean): java.lang.Process = {
    val pb = new ProcessBuilder(argv: _*)
    if (inheritIo) pb.inheritIO()
    pb.start()
  }

  def mapPodPhaseToStatus(pod: Pod): JobStatusType = {
    val phase = Option(pod.getStatus).flatMap(s => Option(s.getPhase)).map(_.trim.toUpperCase).getOrElse("")
    phase match {
      case "PENDING"   => JobStatusType.PENDING
      case "RUNNING"   => JobStatusType.RUNNING
      case "SUCCEEDED" => JobStatusType.SUCCEEDED
      case "FAILED"    => JobStatusType.FAILED
      case _           => JobStatusType.UNKNOWN
    }
  }

  private def pickDriverContainer(statuses: Seq[ContainerStatus]): Option[ContainerStatus] = {
    val nonInit = statuses.filterNot(s => Option(s.getName).exists(_.startsWith("spark-init")))
    nonInit.find(s => Option(s.getName).exists(_.contains("driver"))).orElse(nonInit.headOption)
  }

  def readDriverTermination(pod: Pod): Option[(Int, Option[String])] = {
    val statuses = Option(pod.getStatus)
      .flatMap(s => Option(s.getContainerStatuses))
      .map(_.asScala.toSeq)
      .getOrElse(Nil)
    pickDriverContainer(statuses).flatMap { cs =>
      Option(cs.getState).flatMap(s => Option(s.getTerminated)).map { terminated =>
        val reason = Option(terminated.getReason).map(_.trim).filter(_.nonEmpty)
        (terminated.getExitCode, reason)
      }
    }
  }

  /** Retry while the API server catches up. Throws on multiple matches rather than `items.head`
    * (see block comment above on stale-pod false success).
    */
  private[cloud_k8s] def findDriverPod(
      client: KubernetesClient,
      namespace: String,
      chrononJobId: String,
      attempts: Int = VerifyDriverPodAttempts,
      sleepMs: Long = VerifyDriverPodSleepMs
  ): Option[Pod] = {
    val tries = math.max(1, attempts)
    var i = 0
    while (i < tries) {
      val pods = client
        .pods()
        .inNamespace(namespace)
        .withLabel(ChrononJobIdLabel, chrononJobId)
        .withLabel(SparkRoleLabel, SparkRoleDriver)
        .list()
      val items = Option(pods.getItems).map(_.asScala.toList).getOrElse(Nil)
      if (items.nonEmpty) {
        if (items.size > 1) {
          throw new IllegalStateException(
            s"Multiple driver pods (${items.size}) matched $ChrononJobIdLabel=$chrononJobId in " +
              s"$namespace; chronon-job-id is supposed to be unique per submission, this indicates " +
              "a bug in formatChrononJobId or in label stamping. Refusing to silently pick one."
          )
        }
        return Some(items.head)
      }
      i += 1
      if (i < tries && sleepMs > 0L) {
        try Thread.sleep(sleepMs)
        catch { case _: InterruptedException => Thread.currentThread().interrupt() }
      }
    }
    None
  }

  /** Derive a definitive exit code from a driver pod's observed state.
    *
    * Pod phase is authoritative when terminal, because `spark-submit` is known to occasionally
    * report 0 for a driver pod whose watcher actually observed `Failed`. Returns `None` for
    * non-terminal phases so the caller can fall back to the spark-submit exit code.
    */
  private[cloud_k8s] def driverExitCodeFromPod(pod: Pod): Option[Int] = {
    val phase = Option(pod.getStatus).flatMap(s => Option(s.getPhase)).map(_.trim).getOrElse("")
    val terminated = readDriverTermination(pod)
    phase.toUpperCase match {
      case "SUCCEEDED" => Some(0)
      case "FAILED" =>
        terminated match {
          case Some((code, _)) if code != 0 => Some(code)
          case _                            => Some(1)
        }
      case "RUNNING" | "PENDING" | "UNKNOWN" | "" =>
        // Pod still settling; defer to caller (which will fall back to spark-submit exit code).
        terminated.collect { case (code, _) if code != 0 => code }
      case _ =>
        terminated.map(_._1)
    }
  }

  /** Implementation of [[verifyDriverAndCleanup]] that operates on an externally-managed client,
    * for tests. Production callers should use [[verifyDriverAndCleanup]] which owns the
    * [[KubernetesClient]] lifecycle.
    */
  private[cloud_k8s] def verifyDriverAndCleanupWith(
      client: KubernetesClient,
      namespace: String,
      chrononJobId: String,
      sparkSubmitExitCode: Int,
      attempts: Int = VerifyDriverPodAttempts,
      sleepMs: Long = VerifyDriverPodSleepMs
  ): Int = {
    findDriverPod(client, namespace, chrononJobId, attempts = attempts, sleepMs = sleepMs) match {
      case None =>
        // Pod never landed (or was already deleted); fall back to the spark-submit exit code and
        // log loudly so a false success (spark-submit 0 for an actually-failed driver) is debuggable.
        JobSubmitter.logger.error(
          s"Driver pod not found for $ChrononJobIdLabel=$chrononJobId,$SparkRoleLabel=$SparkRoleDriver in $namespace " +
            s"after $attempts attempts; falling back to spark-submit exit code $sparkSubmitExitCode"
        )
        sparkSubmitExitCode

      case Some(pod) =>
        val podName = Option(pod.getMetadata).flatMap(m => Option(m.getName)).getOrElse(chrononJobId)
        val phase = Option(pod.getStatus).flatMap(s => Option(s.getPhase)).getOrElse("Unknown")
        val termination = readDriverTermination(pod)

        val code = driverExitCodeFromPod(pod) match {
          case Some(c) =>
            if (c != 0) {
              val reason = termination.flatMap(_._2).getOrElse("")
              JobSubmitter.logger.error(
                s"Driver pod $podName terminated unsuccessfully (phase=$phase, exitCode=$c" +
                  (if (reason.nonEmpty) s", reason=$reason)" else ")")
              )
            }
            c
          case None =>
            JobSubmitter.logger.warn(
              s"Driver pod $podName phase=$phase has no terminated container state; " +
                s"falling back to spark-submit exit code $sparkSubmitExitCode"
            )
            sparkSubmitExitCode
        }

        try client.pods().inNamespace(namespace).withName(podName).delete()
        catch { case NonFatal(_) => () }
        code
    }
  }

  def verifyDriverAndCleanup(
      namespace: String,
      chrononJobId: String,
      sparkSubmitExitCode: Int,
      configResolver: () => Config = () => Config.autoConfigure(null)
  ): Int = {
    var client: KubernetesClient = null
    try {
      client = new KubernetesClientBuilder().withConfig(configResolver()).build()
      verifyDriverAndCleanupWith(client, namespace, chrononJobId, sparkSubmitExitCode)
    } catch {
      case NonFatal(e) =>
        JobSubmitter.logger.warn(
          s"K8s verify/cleanup failed (${e.getClass.getSimpleName}): ${Option(e.getMessage).getOrElse("")}; " +
            s"falling back to spark-submit exit code $sparkSubmitExitCode"
        )
        sparkSubmitExitCode
    } finally {
      if (client != null) client.close()
    }
  }

  def runBlockingFromArgs(args: Array[String], configResolver: () => Config = () => Config.autoConfigure(null)): Int = {
    val jobTypeStr = JobSubmitter
      .getArgValue(args, JobTypeArgKeyword)
      .getOrElse(throw new IllegalArgumentException("Missing required argument: " + JobTypeArgKeyword))
    if (!jobTypeStr.equalsIgnoreCase(SparkJobType)) {
      throw new IllegalArgumentException(s"K8sSubmitter only supports --job-type=$SparkJobType, got $jobTypeStr")
    }
    val submitter = fromEnv(configResolver)
    val submissionProps = createSubmissionPropsMap(args)
    val jobProps = JobSubmitter.getModeConfigProperties(args).getOrElse(Map.empty)
    val files = getFilesArgs(args)
    val appName = resolveSparkAppName(submissionProps, args)
    val jobIdUuid = submissionProps.getOrElse(
      JobId,
      throw new IllegalArgumentException(s"Missing required argument: $JobIdArgKeyword")
    )
    val chrononJobId = formatChrononJobId(SparkJob, submitter.namespace, appName, jobIdUuid)
    SubmissionMode.resolve(submissionProps, jobProps) match {
      case SubmissionMode.SparkOperator =>
        runBlockingViaSparkApplicationCrd(
          submitter = submitter,
          submissionProps = submissionProps,
          jobProps = jobProps,
          files = files,
          args = args,
          appName = appName,
          chrononJobId = chrononJobId,
          configResolver = configResolver
        )
      case SubmissionMode.SparkSubmit =>
        val argv = buildSparkSubmitArgv(
          submitter = submitter,
          submissionProperties = submissionProps,
          jobProperties = jobProps,
          files = files,
          rawArgs = args,
          jobType = SparkJob,
          sparkAppName = appName,
          chrononJobId = chrononJobId,
          waitForAppCompletion = true
        )

        JobSubmitter.logger.info(s"Job submitted with ID: $chrononJobId")
        val proc =
          try runSparkSubmitProcess(argv, inheritIo = true)
          catch {
            case e: IOException if e.getMessage != null && e.getMessage.contains("error=2") =>
              JobSubmitter.logger.error(
                s"spark-submit not found at ${submitter.sparkSubmitPath}. Set SPARK_SUBMIT_PATH if not on PATH."
              )
              throw e
          }

        val sparkExit = waitForSparkSubmitExitCode(proc)

        val finalExit = verifyDriverAndCleanup(
          namespace = submitter.namespace,
          chrononJobId = chrononJobId,
          sparkSubmitExitCode = sparkExit,
          configResolver = configResolver
        )
        if (finalExit == 0) JobSubmitter.logger.info("Job completed with status: SUCCESS")
        else JobSubmitter.logger.info("Job completed with status: FAILED")
        finalExit
    }
  }

  /** CRD ingress for async [[K8sSubmitter.submit]]; production impl must apply `chronon-job-id` on the driver. */
  private[cloud_k8s] def submitViaSparkApplicationCrd(
      submitter: K8sSubmitter,
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      envVars: Map[String, String],
      rawArgs: Array[String],
      appName: String,
      chrononJobId: String
  ): String = {
    throw new UnsupportedOperationException(
      s"submission-mode=${SubmissionMode.SparkOperator.name} (SparkApplication CRD) is not implemented yet; " +
        s"use ${SubmissionMode.SparkSubmit.name} or unset ${SubmissionMode.EnvVar} / ${SubmissionMode.PropertyKey}."
    )
  }

  /** CRD ingress for [[runBlockingFromArgs]]; production impl must apply `chronon-job-id` on the driver. */
  private[cloud_k8s] def runBlockingViaSparkApplicationCrd(
      submitter: K8sSubmitter,
      submissionProps: Map[String, String],
      jobProps: Map[String, String],
      files: List[String],
      args: Array[String],
      appName: String,
      chrononJobId: String,
      configResolver: () => Config
  ): Int = {
    throw new UnsupportedOperationException(
      s"submission-mode=${SubmissionMode.SparkOperator.name} (SparkApplication CRD) is not implemented yet; " +
        s"use ${SubmissionMode.SparkSubmit.name} or unset ${SubmissionMode.EnvVar} / ${SubmissionMode.PropertyKey}."
    )
  }

  def main(args: Array[String]): Unit = {
    try {
      System.exit(runBlockingFromArgs(args))
    } catch {
      case e: IllegalStateException if e.getMessage != null && e.getMessage.contains("SPARK_K8S") =>
        JobSubmitter.logger.warn(e.getMessage)
        System.exit(1)
      case e: IllegalArgumentException =>
        JobSubmitter.logger.warn("Invalid K8sSubmitter arguments", e)
        System.exit(1)
      case e: IOException =>
        JobSubmitter.logger.error("K8sSubmitter I/O failure (e.g. spark-submit missing)", e)
        System.exit(1)
      case NonFatal(e) =>
        JobSubmitter.logger.error("K8sSubmitter.main failed", e)
        System.exit(1)
    }
  }
}
