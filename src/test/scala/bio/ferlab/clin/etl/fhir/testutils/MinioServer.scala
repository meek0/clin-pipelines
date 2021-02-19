package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.fhir.testutils.containers.MinioContainer
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.scalatest.{BeforeAndAfterAll, TestSuite}

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable
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

  val inputBucket = s"clin-import"
  val outputBucket = s"clin-repository"
  createBuckets()

  private def createBuckets(): Unit = {
    val alreadyExistingBuckets = s3.listBuckets().asScala.collect { case b if b.getName == inputBucket || b.getName == outputBucket => b.getName }
    val bucketsToCreate = Seq(inputBucket, outputBucket).diff(alreadyExistingBuckets)
    bucketsToCreate.foreach(s3.createBucket)
  }

  def withObjects[T](block: (String, String) => T): Unit = {
    val inputPrefix = s"run_${Random.nextInt(10000)}"
    println(s"Use input prefix $inputPrefix : $minioEndpoint/minio/$inputBucket/$inputPrefix")
    val outputPrefix = s"files_${Random.nextInt(10000)}"
    println(s"Use output prefix $outputPrefix : : $minioEndpoint/minio/$outputBucket/$outputPrefix")
    try {
      block(inputPrefix, outputPrefix)
    } finally {
      deleteRecursively(inputBucket, inputPrefix)
      deleteRecursively(outputBucket, outputPrefix)
    }
  }

  def list(bucket: String, prefix: String): Seq[String] = {
    s3.listObjects(bucket, prefix).getObjectSummaries.asScala.map(_.getKey)
  }

  private def deleteRecursively(bucket: String, prefix: String): Unit = {
    val obj = s3.listObjects(bucket, prefix)
    obj.getObjectSummaries.asScala.foreach { o =>
      s3.deleteObject(bucket, o.getKey)
    }
  }
  def transferFromResources(prefix: String, resource: String, bucket: String = inputBucket): Unit = {
    val transferManager = TransferManagerBuilder.standard.withS3Client(s3).build()
    val transfert = transferManager.uploadDirectory(bucket, prefix, new File(getClass.getResource(s"/$resource").toURI), false);
    transfert.waitForCompletion()
  }
}


trait MinioServerSuite extends MinioServer with TestSuite with BeforeAndAfterAll {

}

object StartMinioServer extends App with MinioServer {
  println(s"Minio is started : $minioEndpoint")
  while (true) {

  }

}
