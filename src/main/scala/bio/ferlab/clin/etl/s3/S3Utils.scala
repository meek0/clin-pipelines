package bio.ferlab.clin.etl.s3

import bio.ferlab.clin.etl.conf.AWSConf
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.AwsClient
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, HeadObjectRequest, NoSuchKeyException, PutObjectRequest}
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Client, S3Configuration}

import java.net.URI
import java.time.Duration
import java.time.Duration.ofMinutes
import java.time.temporal.TemporalUnit

object S3Utils {

  def buildS3Client(conf: AWSConf): S3Client = {
    val s3Creds: StaticCredentialsProvider = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(conf.accessKey, conf.secretKey)
    )
    val endpoint = URI.create(conf.endpoint)
    val confBuilder: S3Configuration = software.amazon.awssdk.services.s3.S3Configuration.builder()
      .pathStyleAccessEnabled(conf.pathStyleAccess)
      .build()
    val s3: S3Client = S3Client.builder()
      .credentialsProvider(s3Creds)
      .endpointOverride(endpoint)
      .region(Region.US_EAST_1)
      .serviceConfiguration(confBuilder)
      .httpClientBuilder(ApacheHttpClient.builder()
        .connectionTimeout(ofMinutes(60))
        .socketTimeout(ofMinutes(60))
      )
      .build()
    s3
  }

  def buildAsyncS3Client(conf: AWSConf) = {
    val s3Creds: StaticCredentialsProvider = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(conf.accessKey, conf.secretKey)
    )
    val endpoint = URI.create(conf.endpoint)
    val s3: S3AsyncClient = S3AsyncClient.crtBuilder()
      .credentialsProvider(s3Creds)
      .endpointOverride(endpoint)
      .region(Region.US_EAST_1)
      .targetThroughputInGbps(20.0)
      .checksumValidationEnabled(true)
      .forcePathStyle(conf.pathStyleAccess)
      .httpConfiguration(S3CrtHttpConfiguration.builder()
        .connectionTimeout(Duration.ofMinutes(60))
        .build())
      .minimumPartSizeInBytes(8 * 1024 *1024)
      .build()
    s3
  }


  def getContent(bucket: String, key: String)(implicit s3Client: S3Client): String = {
    val objectRequest = GetObjectRequest
      .builder()
      .key(key)
      .bucket(bucket)
      .build()
    new String(s3Client.getObject(objectRequest).readAllBytes())
  }

  def writeContent(bucket: String, key: String, content: String)(implicit s3Client: S3Client): Unit = {
    val objectRequest = PutObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()
    s3Client.putObject(objectRequest, RequestBody.fromString(content))
  }

  def exists(bucket: String, key: String)(implicit s3Client: S3Client): Boolean =
    try {
      s3Client.headObject(HeadObjectRequest.builder.bucket(bucket).key(key).build)
      true
    } catch {
      case _: NoSuchKeyException =>
        false
    }

}
