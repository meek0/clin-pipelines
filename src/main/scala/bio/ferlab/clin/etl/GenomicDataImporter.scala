package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.task.{CheckS3DataTask, Configuration, LoadHapiFhirDataTask}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.slf4j.{Logger, LoggerFactory}

object GenomicDataImporter extends App {

  val LOGGER: Logger = LoggerFactory.getLogger(GenomicDataImporter.getClass)

  // Parse program arguments
  val (task: String, batchId: Option[String]) = args match {
    case Array(task) => (task, None)
    case Array(task, batchId) => (task, Some(batchId))
    case _ => {
      LOGGER.error("Usage: GenomicDataImporter task_name [batch_id]")
      System.exit(-1)
    }
  }

  val credentials: BasicAWSCredentials = new BasicAWSCredentials(Configuration.accessKey, Configuration.secretKey)

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new EndpointConfiguration(Configuration.serviceEndpoint, Configuration.signinRegion))
    .withCredentials(new AWSStaticCredentialsProvider(credentials)).build()

  LOGGER.info(s"Executing bio.ferlab.clin.etl.task : $task")

  var returnCode: Int = task match {
    case "validate-files-on-s3" => {
      try {

        val files: List[String] = if (!batchId.isEmpty)
          CheckS3DataTask.run(Configuration.bucket, batchId.get, s3Client)
        else
          CheckS3DataTask.run(Configuration.bucket, s3Client)

        if (CheckS3DataTask.validateFiles(files)) {
          // Since all required files are present, suppress the _SUCCESS
          // in order to prevent the next Airflow run to process this same batch a second time
          LOGGER.info(s"Deleting _SUCCESS for batch ${batchId.get}")
          s3Client.deleteObject(Configuration.bucket, s"${batchId.get}_SUCCESS")
          0
        } else -1
      } catch {
        case e: AmazonS3Exception =>
          e.getErrorCode match {
            case "NoSuchBucket" =>
              LOGGER.error(s"The '${Configuration.bucket}' bucket does not exist.")
              -1
            case _ =>
              LOGGER.error(s"An S3 error occurred.", e)
              -1
          }
        case e: Throwable =>
          LOGGER.error("An error occurred", e);
          -1
      }
    }
    case "load-metadata-in-bio.ferlab.clin.etl.fhir" => {
      try {
        LoadHapiFhirDataTask.run(Configuration.fhirServerBase)
        LOGGER.info(s"Done loading specimens in HAPI Fhir")
        0
      } catch {
        case _: Throwable => -1
      }
    }
    case "extract-bio.ferlab.clin.etl.fhir-data-for-etl" => {
      LOGGER.info(s"Done exporting patients and specimens from HAPI Fhir")
      0
    }
    case _ => {
      LOGGER.error(s"No such bio.ferlab.clin.etl.task: $task\n Available options are :\n \t[ check-new-files-on-s3 | load-metadata-in-bio.ferlab.clin.etl.fhir | extract-bio.ferlab.clin.etl.fhir-data-for-etl ]")
      -1
    }
  }
  System.exit(returnCode)
}
