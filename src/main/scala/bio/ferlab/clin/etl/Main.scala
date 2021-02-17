package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.model.{FileEntry, Metadata}
import bio.ferlab.clin.etl.task.LoadHapiFhirDataTask.fhirContext
import bio.ferlab.clin.etl.task.{BuildBundle, CheckS3DataTask}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Invalid
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.hl7.fhir.r4.model.Bundle

object Main extends App {

  val Array(bucket, prefix, bucketDest, prefixDest) = args
  val fhirServerUrl = sys.env.getOrElse("fhir.server.url", "http://localhost:18080/fhir")

  import com.amazonaws.ClientConfiguration

  val clientConfiguration = new ClientConfiguration
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")
  implicit val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .build()

  implicit val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], fhirServerUrl)
  implicit val client: IGenericClient = fhirContext.newRestfulGenericClient(fhirServerUrl)
  //TODO :
  //  val authToken: String = getAuthToken()
  //    val hapiFhirInterceptor: AuthTokenInterceptor = new AuthTokenInterceptor(authToken)
  //    clinClient.registerInterceptor(hapiFhirInterceptor)
  //    client.registerInterceptor(hapiFhirInterceptor)

  val metadata = Metadata.validateMetadataFile(bucket, prefix)
  val result: Validated[NonEmptyList[String], Bundle] = metadata.andThen { m: Metadata =>
    val fileEntries = CheckS3DataTask.loadFileEntries(bucket, prefix)
    (BuildBundle.validate(m, fileEntries), CheckS3DataTask.validate(m, fileEntries))
      .mapN { (bundle, files) =>
        try {
          FileEntry.copyFiles(files, bucketDest, prefixDest)
          bundle.save()
        } catch {
          case e: Exception =>
            FileEntry.revert(files, bucketDest, prefixDest)
            throw e
        }
      }
  }

  result match {
    case Invalid(NonEmptyList(h, t)) =>
      println(h)
      t.foreach(println)
    case Validated.Valid(_) => println("Success!")
  }
}
