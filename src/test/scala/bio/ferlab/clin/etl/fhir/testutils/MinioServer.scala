package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.fhir.testutils.containers.MinioContainer
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.scalatest.{BeforeAndAfterAll, TestSuite}

import scala.util.Random

trait MinioServer {
  private val minioPort = MinioContainer.startIfNotRunning()
  protected val minioEndpoint = s"http://localhost:${minioPort}"
  implicit val s3: AmazonS3 = AmazonS3ClientBuilder.standard
    .withEndpointConfiguration(new EndpointConfiguration(minioEndpoint, Regions.US_EAST_1.name()))
    .withPayloadSigningEnabled(false)
    .withChunkedEncodingDisabled(true)
    .withPathStyleAccessEnabled(true)
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(MinioContainer.accessKey, MinioContainer.secretKey)))
    .build

  def withBuckets[T](block: (String, String) => T): Unit = {
    val inputBucket = s"clin-import-${Random.nextInt(10000)}"
    val outputBucket = s"clin-repository-${Random.nextInt(10000)}"
    s3.createBucket(inputBucket)
    s3.createBucket(outputBucket)
    block(inputBucket, outputBucket)
//    s3.deleteBucket(inputBucket)
//    s3.deleteBucket(outputBucket)
  }
}

trait MinioServerSuite extends MinioServer with TestSuite with BeforeAndAfterAll {

}

object StartMinioServer extends App with MinioServer {
  println(s"Minio is started : $minioEndpoint")
  while (true) {

  }

}
