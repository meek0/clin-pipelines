package bio.ferlab.clin.etl.s3

import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object S3Client {

  def buildS3Client(): AmazonS3 = {
    val clientConfiguration = new ClientConfiguration
    clientConfiguration.setSignerOverride("AWSS3V4SignerType")
    AmazonS3ClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(sys.env("AWS_ENDPOINT"), Regions.US_EAST_1.name()))
      .withPathStyleAccessEnabled(true)
      .withClientConfiguration(clientConfiguration)
      .build()
  }
}
