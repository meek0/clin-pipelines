package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.model.Metadata
import bio.ferlab.clin.etl.task.LoadHapiFhirDataTask.fhirContext
import bio.ferlab.clin.etl.task.{BuildBundle, CheckS3Data}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Invalid
import cats.data.{NonEmptyList, Validated}
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object Main extends App {

  val Array(bucket, prefix, bucketDest, prefixDest) = args
  val fhirServerUrl = sys.env.getOrElse("fhir.server.url", "http://localhost:18080/fhir")

  import com.amazonaws.ClientConfiguration

  val clientConfiguration = new ClientConfiguration
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")
  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .build()

  val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], fhirServerUrl)
  val client: IGenericClient = fhirContext.newRestfulGenericClient(fhirServerUrl)
  //TODO :
  //  val authToken: String = getAuthToken()
  //    val hapiFhirInterceptor: AuthTokenInterceptor = new AuthTokenInterceptor(authToken)
  //    clinClient.registerInterceptor(hapiFhirInterceptor)
  //    client.registerInterceptor(hapiFhirInterceptor)

  val result = run(bucket, prefix, bucketDest, prefixDest)(s3Client, client, clinClient)

  result match {
    case Invalid(NonEmptyList(h, t)) =>
      println(h)
      t.foreach(println)
      System.exit(-1)
    case Validated.Valid(_) => println("Success!")
  }

  def run(inputBucket: String, inputPrefix: String, outputBucket: String, outputPrefix: String)(implicit s3: AmazonS3, client: IGenericClient, clinFhirClient: IClinFhirClient) = {
    val metadata = Metadata.validateMetadataFile(inputBucket, inputPrefix)
    metadata.andThen { m: Metadata =>
      val fileEntries = CheckS3Data.loadFileEntries(inputBucket, inputPrefix)
      (BuildBundle.validate(m, fileEntries), CheckS3Data.validateFileEntries(m, fileEntries))
        .mapN { (bundle, files) =>
          try {
            CheckS3Data.copyFiles(files, outputBucket, outputPrefix)
            bundle.save()
          } catch {
            case e: Exception =>
              CheckS3Data.revert(files, outputBucket, outputPrefix)
              throw e
          }
        }
    }
  }


}
