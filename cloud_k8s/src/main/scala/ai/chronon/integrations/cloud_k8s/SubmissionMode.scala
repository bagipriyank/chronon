package ai.chronon.integrations.cloud_k8s

/** K8s Spark dispatch: native spark-submit vs SparkApplication CRD (stub). */
sealed trait SubmissionMode { def name: String }

object SubmissionMode {

  case object SparkSubmit extends SubmissionMode { val name: String = "spark-submit" }

  case object SparkOperator extends SubmissionMode { val name: String = "spark-operator" }

  val Default: SubmissionMode = SparkSubmit

  val EnvVar: String = "CHRONON_K8S_SUBMISSION_MODE"

  val PropertyKey: String = "submission-mode"

  def parse(raw: String): SubmissionMode = Option(raw).map(_.trim.toLowerCase).getOrElse("") match {
    case "" | "spark-submit" => SparkSubmit
    case "spark-operator"    => SparkOperator
    case other =>
      throw new IllegalArgumentException(
        s"Unknown $EnvVar value '$other'; expected one of: ${SparkSubmit.name}, ${SparkOperator.name}"
      )
  }

  def resolve(submissionProperties: Map[String, String], jobProperties: Map[String, String]): SubmissionMode =
    submissionProperties
      .get(PropertyKey)
      .orElse(jobProperties.get(PropertyKey))
      .orElse(sys.env.get(EnvVar))
      .map(parse)
      .getOrElse(Default)

  def resolve(submissionProperties: Map[String, String]): SubmissionMode =
    resolve(submissionProperties, Map.empty)
}
