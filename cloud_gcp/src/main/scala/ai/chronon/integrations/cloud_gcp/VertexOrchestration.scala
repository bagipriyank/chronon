package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.commonConstants.{END_DS_KEYWORD, INPUT_TABLE_KEYWORD, START_DS_KEYWORD}
import ai.chronon.api.{JobStatusType, PartitionSpec, ResourceConfig, ServingContainerConfig, TrainingSpec}
import ai.chronon.online.metrics.{FlexibleExecutionContext, Metrics}
import ai.chronon.online.{DeployModelRequest, ModelJobStatus, TrainingRequest}
import com.google.api.core.ApiFuture
import com.google.cloud.aiplatform.v1._
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._

class VertexOrchestration(project: String, location: String) extends Serializable {
  import VertexOrchestration._

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Lazy initialization of SDK clients
  @transient private lazy val jobServiceClient: JobServiceClient = {
    val settings = JobServiceSettings
      .newBuilder()
      .setEndpoint(s"$location-aiplatform.googleapis.com:443")
      .build()
    JobServiceClient.create(settings)
  }

  @transient private lazy val endpointServiceClient: EndpointServiceClient = {
    val settings = EndpointServiceSettings
      .newBuilder()
      .setEndpoint(s"$location-aiplatform.googleapis.com:443")
      .build()
    EndpointServiceClient.create(settings)
  }

  @transient private lazy val modelServiceClient: ModelServiceClient = {
    val settings = ModelServiceSettings
      .newBuilder()
      .setEndpoint(s"$location-aiplatform.googleapis.com:443")
      .build()
    ModelServiceClient.create(settings)
  }

  implicit lazy val ec: ExecutionContextExecutor =
    FlexibleExecutionContext.buildExecutionContext(
      Metrics.Context(Metrics.Environment.Orchestrator).withSuffix("threadpool"))

  // Forms a training request using the Vertex AI SDK
  // Based on: https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.customJobs
  def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = {
    Future {
      val model = trainingRequest.model
      require(model.getTrainingConf != null, "Model must have training configuration")
      require(model.getModelArtifactBaseUri != null, "Model must have modelArtifactBaseUri set")

      val trainingSpec = model.getTrainingConf
      val inferenceSpec = model.getInferenceSpec
      val modelName = Option(inferenceSpec.getModelBackendParams.get("model_name"))
        .getOrElse(throw new IllegalArgumentException("model_name is required in modelBackendParams"))
      val version = model.metaData.version
      val date = trainingRequest.date

      // Build paths according to specification
      val modelArtifactBaseUri = model.getModelArtifactBaseUri
      val pythonPackageUri = s"$modelArtifactBaseUri/builds/$modelName-$version.tar.gz"
      val outputDir = s"$modelArtifactBaseUri/training_output/$modelName-$version/$date"

      logger.info(
        s"Submitting training job for $modelName-$version; Python pkg: $pythonPackageUri; Model output dir: $outputDir")

      val customJob = buildCustomJob(trainingSpec, modelName, version, date, pythonPackageUri, outputDir)

      // Submit the job
      try {
        val parent = LocationName.of(project, location)
        val createdJob = jobServiceClient.createCustomJob(parent, customJob)
        val jobName = createdJob.getName
        logger.info(s"Training job submitted successfully: $jobName")

        // CustomJob return the direct job id so will extract.
        // Ex: `projects/703996152583/locations/us-central1/customJobs/6897601805316784128`
        val customJobId = jobName.split("/").last
        logger.info(
          s"Follow job progress at: https://console.cloud.google.com/vertex-ai/locations/$location/training/$customJobId")
        jobName
      } catch {
        case e: Exception =>
          logger.error(s"Failed to submit training job: ${e.getMessage}", e)
          throw new RuntimeException(s"Failed to submit training job: ${e.getMessage}", e)
      }
    }
  }

  def createEndpoint(endpointName: String): Future[String] = {
    findEndpointByName(endpointName) match {
      case Some(endpointResourceName) =>
        logger.info(s"Found existing endpoint: $endpointResourceName")
        Future.successful(endpointResourceName)
      case None =>
        logger.info(s"Creating new endpoint: $endpointName")
        createNewEndpoint(endpointName)
    }
  }

  // display names are what we have users specify which are different from the endpoint resource names
  // so we need to list and find the right one
  def findEndpointByName(endpointName: String): Option[String] = {
    try {
      val parent = LocationName.of(project, location)

      // List all endpoints in the location
      val listRequest = ListEndpointsRequest
        .newBuilder()
        .setParent(parent.toString)
        .build()

      val endpoints = endpointServiceClient.listEndpoints(listRequest).iterateAll().asScala

      // Find endpoint with matching display name
      endpoints.find(_.getDisplayName == endpointName).map(_.getName)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to list endpoints: ${e.getMessage}", e)
        throw new RuntimeException(s"Failed to list endpoints: ${e.getMessage}", e)
    }
  }

  private def createNewEndpoint(endpointName: String): Future[String] = {
    try {
      val parent = LocationName.of(project, location)
      val endpoint = Endpoint
        .newBuilder()
        .setDisplayName(endpointName)
        .build()

      val operation = endpointServiceClient.createEndpointAsync(parent, endpoint)
      val operationFuture = googleFutureToScalaFuture(operation)
      operationFuture.map { createdEndpoint =>
        val endpointResourceName = createdEndpoint.getName
        logger.info(s"Endpoint created: $endpointResourceName")
        endpointResourceName
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create endpoint: ${e.getMessage}", e)
        throw new RuntimeException(s"Failed to create endpoint: ${e.getMessage}", e)
    }
  }

  // Deploys a model to an endpoint. This proceeds in three steps:
  // 1) Find the endpoint (should already exist)
  // 2) Upload model to registry
  // 3) Deploy model to endpoint
  def deployModel(deployModelRequest: DeployModelRequest): Future[String] = {
    val model = deployModelRequest.model
    val version = deployModelRequest.version
    val date = deployModelRequest.date
    val deploymentSpec = model.getDeploymentConf
    val resourceConfig = deploymentSpec.getResourceConfig
    val containerConfig = deploymentSpec.getContainerConfig
    val modelName = Option(model.getInferenceSpec.getModelBackendParams.get("model_name"))
      .getOrElse(throw new IllegalArgumentException("model_name is required in modelBackendParams"))

    // lookup endpoint
    val endpointConfig = deploymentSpec.getEndpointConfig
    val endpointFuture = findEndpointByName(endpointConfig.getEndpointName) match {
      case Some(endpointResourceName) =>
        logger.info(s"Found existing endpoint: $endpointResourceName")
        Future.successful(endpointResourceName)
      case None =>
        Future.failed(
          new RuntimeException(
            s"Endpoint ${endpointConfig.getEndpointName} not found. Please create it first using createEndpoint."))
    }

    // Build model artifact URI
    val modelArtifactBaseUri = model.getModelArtifactBaseUri
    val modelArtifactUri = s"$modelArtifactBaseUri/training_output/$modelName-$version/$date/model"

    // upload model then deploy
    for {
      endpointResourceName <- endpointFuture
      modelResourceName <- uploadModel(containerConfig, modelName, version, modelArtifactUri)
      deploymentId <- deployModelToEndpoint(endpointResourceName, modelResourceName, modelName, resourceConfig)
    } yield deploymentId
  }

  // upload model to vertex model registry
  // based on: https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.models/upload
  private def uploadModel(containerConfig: ai.chronon.api.ServingContainerConfig,
                          modelName: String,
                          version: String,
                          modelArtifactUri: String): Future[String] = {
    try {
      logger.info(
        s"Uploading model: $modelName-$version; Artifact URI: $modelArtifactUri; Container Image: ${containerConfig.getImage}")

      val model = buildModel(containerConfig, modelName, version, modelArtifactUri)

      val parent = LocationName.of(project, location)
      val uploadOperation = modelServiceClient.uploadModelAsync(parent, model)
      val uploadOperationFuture = googleFutureToScalaFuture(uploadOperation)
      uploadOperationFuture.map { uploadResponse =>
        val modelResourceName = uploadResponse.getModel
        logger.info(s"Model upload completed: $modelResourceName")
        modelResourceName
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to upload model: ${e.getMessage}", e)
        Future.failed(new RuntimeException(s"Failed to upload model: ${e.getMessage}", e))
    }
  }

  // deploy model to endpoint - returns the operation name which we use to track its progress
  // This currently is fairly simple and doesn't cover more involved scenarios like blue/green, traffic rampups etc.
  // based on: https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.endpoints/deployModel
  private def deployModelToEndpoint(endpointResourceName: String,
                                    modelResourceName: String,
                                    deployedModelDisplayName: String,
                                    resourceConfig: ResourceConfig): Future[String] = {
    Future {
      try {
        logger.info(s"""
             |Deploying model to endpoint. Details:
             |Model: $modelResourceName;
             |Endpoint: $endpointResourceName;
             |Replicas: ${resourceConfig.getMinReplicaCount}-${resourceConfig.getMaxReplicaCount}
             |Machine type: ${resourceConfig.getMachineType}
             |""".stripMargin)

        val deployedModel = buildDeployedModel(modelResourceName, deployedModelDisplayName, resourceConfig)

        val endpointName = EndpointName.parse(endpointResourceName)

        // Traffic split - route 100% traffic to the new deployment (deployed model ID "0")
        val trafficSplit = Map[String, Integer]("0" -> Integer.valueOf(100)).asJava

        val deployOperation = endpointServiceClient.deployModelAsync(endpointName, deployedModel, trafficSplit)
        val operationName = deployOperation.getName
        logger.info(s"Model deployment initiated. Operation: $operationName")
        operationName
      } catch {
        case e: Exception =>
          logger.error(s"Failed to deploy model: ${e.getMessage}", e)
          throw new RuntimeException(s"Failed to deploy model: ${e.getMessage}", e)
      }
    }
  }

  // Check the status of a training job
  // based on: https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.customJobs/get
  def getTrainingJobStatus(jobName: String): Future[ModelJobStatus] = {
    Future {
      try {
        val customJobName = CustomJobName.parse(jobName)
        val job = jobServiceClient.getCustomJob(customJobName)

        val state = job.getState
        val statusType = parseJobState(state)

        val message = statusType match {
          case JobStatusType.FAILED =>
            val errorMsg = if (job.hasError) job.getError.getMessage else state.toString
            s"Training job failed: $errorMsg"
          case JobStatusType.SUCCEEDED => s"Training job succeeded: $state"
          case JobStatusType.CANCELLED => s"Training job cancelled: $state"
          case JobStatusType.RUNNING   => s"Training job running: $state"
          case JobStatusType.PENDING   => s"Training job pending: $state"
          case _                       => s"Training job status unknown: $state"
        }

        ModelJobStatus(statusType, message)
      } catch {
        case e: Exception =>
          logger.error(s"Failed to get training job status: ${e.getMessage}", e)
          ModelJobStatus(JobStatusType.UNKNOWN, s"Failed to get training job status: ${e.getMessage}")
      }
    }
  }

  // Check the status of a long-running operation (e.g., model deployment)
  // based on: https://docs.cloud.google.com/vertex-ai/docs/reference/rest/Shared.Types/ListOperationsResponse#Operation
  def getOperationStatus(operationName: String): Future[ModelJobStatus] = {
    Future {
      try {
        val operation = endpointServiceClient.getOperationsClient.getOperation(operationName)

        if (operation.getDone) {
          if (operation.hasError) {
            val error = operation.getError
            ModelJobStatus(JobStatusType.FAILED, s"Operation failed: ${error.getMessage}")
          } else {
            ModelJobStatus(JobStatusType.SUCCEEDED, "Operation completed successfully")
          }
        } else {
          ModelJobStatus(JobStatusType.RUNNING, "Operation in progress")
        }
      } catch {
        case e: Exception =>
          logger.error(s"Failed to get operation status: ${e.getMessage}", e)
          ModelJobStatus(JobStatusType.UNKNOWN, s"Failed to get operation status: ${e.getMessage}")
      }
    }
  }

  def close(): Unit = {
    try {
      if (jobServiceClient != null) jobServiceClient.close()
      if (endpointServiceClient != null) endpointServiceClient.close()
      if (modelServiceClient != null) modelServiceClient.close()
    } catch {
      case e: Exception =>
        logger.warn(s"Error closing SDK clients: ${e.getMessage}", e)
    }
  }
}

object VertexOrchestration {

  def googleFutureToScalaFuture[T](apiFuture: ApiFuture[T]): Future[T] = {
    val completableFuture = ApiFutureUtils.toCompletableFuture(apiFuture)
    FutureConverters.toScala(completableFuture)
  }

  def buildCustomJob(trainingSpec: TrainingSpec,
                     modelName: String,
                     version: String,
                     date: String,
                     pythonPackageUri: String,
                     outputDir: String): CustomJob = {
    // Build PythonPackageSpec
    val pythonModule = Option(trainingSpec.getPythonModule).getOrElse("trainer.train")
    val pythonPackageSpecBuilder = PythonPackageSpec
      .newBuilder()
      .setExecutorImageUri(trainingSpec.getImage)
      .addPackageUris(pythonPackageUri)
      .setPythonModule(pythonModule)

    // Add job configs as args
    if (trainingSpec.getJobConfigs != null) {
      trainingSpec.getJobConfigs.asScala.foreach { case (key, value) =>
        pythonPackageSpecBuilder.addArgs(s"--$key=$value")
      }
    }

    pythonPackageSpecBuilder.addArgs(s"${INPUT_TABLE_KEYWORD}=${trainingSpec.getTrainingDataSource.table}")
    val startDs = PartitionSpec.daily.minus(date, trainingSpec.trainingDataWindow)
    val endDs = date
    pythonPackageSpecBuilder.addArgs(s"$START_DS_KEYWORD=$startDs")
    pythonPackageSpecBuilder.addArgs(s"$END_DS_KEYWORD=$endDs")

    // Build MachineSpec
    val resourceConfig = trainingSpec.getResourceConfig
    val machineSpecBuilder = MachineSpec.newBuilder()
    if (resourceConfig != null) {
      machineSpecBuilder.setMachineType(resourceConfig.getMachineType)
    }

    // Build WorkerPoolSpec
    val workerPoolSpecBuilder = WorkerPoolSpec
      .newBuilder()
      .setPythonPackageSpec(pythonPackageSpecBuilder.build())
      .setMachineSpec(machineSpecBuilder.build())

    // Set replica count if specified
    if (resourceConfig != null && resourceConfig.isSetMinReplicaCount) {
      workerPoolSpecBuilder.setReplicaCount(resourceConfig.getMinReplicaCount)
    }

    // Build JobSpec
    val jobSpecBuilder = CustomJobSpec
      .newBuilder()
      .addWorkerPoolSpecs(workerPoolSpecBuilder.build())

    // Set base output directory
    val baseOutputDirectory = GcsDestination
      .newBuilder()
      .setOutputUriPrefix(outputDir)
      .build()
    jobSpecBuilder.setBaseOutputDirectory(baseOutputDirectory)

    // Generate job name
    val timestamp = System.currentTimeMillis()
    val jobDisplayName = s"${modelName}_${version}_${date}_$timestamp"

    // Build CustomJob
    CustomJob
      .newBuilder()
      .setDisplayName(jobDisplayName)
      .setJobSpec(jobSpecBuilder.build())
      .build()
  }

  def buildModel(containerConfig: ServingContainerConfig,
                 modelName: String,
                 version: String,
                 modelArtifactUri: String): Model = {
    // Build container spec
    val containerSpecBuilder = ModelContainerSpec
      .newBuilder()
      .setImageUri(containerConfig.getImage)

    val healthRoute = Option(containerConfig.getServingHealthRoute).getOrElse("/health")
    val predictRoute = Option(containerConfig.getServingPredictRoute).getOrElse("/predict")
    containerSpecBuilder.setHealthRoute(healthRoute)
    containerSpecBuilder.setPredictRoute(predictRoute)

    // Add environment variables if present
    if (containerConfig.getServingContainerEnvVars != null && !containerConfig.getServingContainerEnvVars.isEmpty) {
      containerConfig.getServingContainerEnvVars.asScala.foreach { case (key, value) =>
        containerSpecBuilder.addEnv(EnvVar.newBuilder().setName(key).setValue(value).build())
      }
    }

    // Build the model
    Model
      .newBuilder()
      .setDisplayName(s"$modelName-$version")
      .setArtifactUri(modelArtifactUri)
      .setContainerSpec(containerSpecBuilder.build())
      .build()
  }

  def buildDeployedModel(modelResourceName: String,
                         deployedModelDisplayName: String,
                         resourceConfig: ResourceConfig): DeployedModel = {
    // Build machine spec
    val machineSpec = MachineSpec
      .newBuilder()
      .setMachineType(resourceConfig.getMachineType)
      .build()

    // Build dedicated resources
    val dedicatedResources = DedicatedResources
      .newBuilder()
      .setMachineSpec(machineSpec)
      .setMinReplicaCount(resourceConfig.getMinReplicaCount)
      .setMaxReplicaCount(resourceConfig.getMaxReplicaCount)
      .build()

    // Build deployed model
    DeployedModel
      .newBuilder()
      .setModel(modelResourceName)
      .setDisplayName(deployedModelDisplayName)
      .setDedicatedResources(dedicatedResources)
      .build()
  }

  def parseJobState(state: JobState): JobStatusType = {
    state match {
      case JobState.JOB_STATE_SUCCEEDED                           => JobStatusType.SUCCEEDED
      case JobState.JOB_STATE_FAILED                              => JobStatusType.FAILED
      case JobState.JOB_STATE_CANCELLED                           => JobStatusType.CANCELLED
      case JobState.JOB_STATE_RUNNING                             => JobStatusType.RUNNING
      case JobState.JOB_STATE_PENDING | JobState.JOB_STATE_QUEUED => JobStatusType.PENDING
      case _                                                      => JobStatusType.UNKNOWN
    }
  }
}
