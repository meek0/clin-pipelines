package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.fhir.testutils.containers.OurLocalStackContainer
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.testcontainers.containers.localstack.LocalStackContainer.Service

trait LocalStackT {
  OurLocalStackContainer.startIfNotRunning()
  implicit val s3: AmazonS3 = AmazonS3ClientBuilder.standard.withEndpointConfiguration(OurLocalStackContainer.localStackContainer.getEndpointConfiguration(Service.S3)).withCredentials(OurLocalStackContainer.localStackContainer.getDefaultCredentialsProvider).build
}

object StartLocalStack extends App with LocalStackT {
  println("Whole stack is started")
  while (true) {

  }

}
