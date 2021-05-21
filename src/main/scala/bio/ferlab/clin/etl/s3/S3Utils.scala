package bio.ferlab.clin.etl.s3

import bio.ferlab.clin.etl.task.AWSConf

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.net.URI

object S3Utils {


  def buildS3Client(conf: AWSConf): S3Client = {
    val confBuilder: S3Configuration = software.amazon.awssdk.services.s3.S3Configuration.builder()
      .pathStyleAccessEnabled(conf.pathStyleAccess)
      .build()
    val staticCredentialsProvider: StaticCredentialsProvider = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(conf.accessKey, conf.secretKey)
    )
    val endpoint = URI.create(conf.endpoint)
    val s3: S3Client = S3Client.builder()
      .credentialsProvider(staticCredentialsProvider)
      .endpointOverride(endpoint)
      .serviceConfiguration(confBuilder).build()
    s3
  }
}
