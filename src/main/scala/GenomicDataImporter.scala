import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.slf4j.{Logger, LoggerFactory}

object GenomicDataImporter extends App {
  val Array(task) = args

  val LOGGER: Logger = LoggerFactory.getLogger(GenomicDataImporter.getClass)

  val credentials: BasicAWSCredentials = new BasicAWSCredentials(Configuration.accessKey, Configuration.secretKey)

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new EndpointConfiguration(Configuration.serviceEndpoint, Configuration.signinRegion))
    .withCredentials(new AWSStaticCredentialsProvider(credentials)).build()

  LOGGER.info(s"Executing task : $task")

  var returnCode: Int = task match {
    //Soit fichier success pour trigger batch

    case "check-new-files-on-s3" => {
      try{
        val files:List[String] = CheckS3DataTask.run(Configuration.bucket, s3Client)
        LOGGER.info(s"Found ${files.size} files on S3 bucket 'clin'")

        if (files.nonEmpty) 0 else -1
      }catch{
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
    case "load-metadata-in-fhir" => {
      LOGGER.info(s"Done loading specimens in HAPI Fhir")
      0
    }
    case "extract-fhir-data-for-etl" => {
      LOGGER.info(s"Done exporting patients and specimens from HAPI Fhir")
      0
    }
    /*case "launch-etl" => {
      // This will be done directly from Airflow
    }*/
    case _ => {
      LOGGER.error(s"No such task: $task\n Available options are :\n \t[ check-new-files-on-s3 | load-metadata-in-fhir | extract-fhir-data-for-etl ]")
      -1
    }
  }
  System.exit(returnCode)
}
