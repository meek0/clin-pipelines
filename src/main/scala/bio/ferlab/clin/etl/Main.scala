package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.model.Metadata
import bio.ferlab.clin.etl.s3.S3Client.buildS3Client
import bio.ferlab.clin.etl.task.{BuildBundle, CheckS3Data}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Invalid
import cats.data.{NonEmptyList, Validated}
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.amazonaws.services.s3.AmazonS3

object Main extends App {

  val Array(bucket, prefix, bucketDest, prefixDest) = args

  val s3Client: AmazonS3 = buildS3Client()

  val (clinClient, client) = buildFhirClients()

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
      val rawFileEntries = CheckS3Data.loadRawFileEntries(inputBucket, inputPrefix)
      val fileEntries = CheckS3Data.loadFileEntries(m, rawFileEntries)
      (BuildBundle.validate(m, fileEntries), CheckS3Data.validateFileEntries(rawFileEntries, fileEntries))
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
