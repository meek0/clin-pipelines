package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.Scripts.args
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, Metadata, TBundle}
import bio.ferlab.clin.etl.task.fileimport.{BuildBundle, CheckS3Data}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits.{catsSyntaxTuple2Semigroupal, catsSyntaxValidatedId}
import org.hl7.fhir.r4.model.Bundle
import software.amazon.awssdk.services.s3.S3Client


object FileImport extends App {
  withSystemExit {
    withLog {
      withConf { conf =>
        val batch = args.head
        val params = if (args.length > 1) args.tail.map(_.toString) else Array.empty[String]
        val dryRun = if (params.length > 0) params(0).toBoolean else false
        val full = if (params.length > 1) params(1).toBoolean else false
        implicit val s3Client: S3Client = buildS3Client(conf.aws)
        val (clinClient, client) = buildFhirClients(conf.fhir, conf.keycloak)
        val bucket = conf.aws.bucketName
        withReport(bucket, batch) { reportPath =>
          run(bucket, batch, conf.aws.outputBucketName, conf.aws.outputPrefix, reportPath, dryRun, full)(s3Client, client, clinClient, conf.ferload)
        }
      }
    }
  }

  def writeAheadLog(inputBucket: String, reportPath: String, bundle: TBundle, files: Seq[FileEntry])(implicit s3: S3Client, client: IGenericClient): Unit = {
    S3Utils.writeContent(inputBucket, s"$reportPath/bundle.json", bundle.print())
    val filesToCSV = files.map(f => s"${f.key},${f.id}").mkString("\n")
    S3Utils.writeContent(inputBucket, s"$reportPath/files.csv", filesToCSV)
  }

  def run(inputBucket: String, batch: String, outputBucket: String, outputPrefix: String, reportPath: String, dryRun: Boolean, full: Boolean)(implicit s3: S3Client, client: IGenericClient, clinFhirClient: IClinFhirClient, ferloadConf: FerloadConf): ValidationResult[Bundle] = {
    val metadata: ValidatedNel[String, Metadata] = Metadata.validateMetadataFile(inputBucket, batch, full)
    metadata.andThen { m: Metadata =>
      val rawFileEntries = CheckS3Data.loadRawFileEntries(inputBucket, batch)
      val fileEntries = CheckS3Data.loadFileEntries(m, rawFileEntries, outputPrefix)
      val results = (BuildBundle.validate(m, fileEntries, batch), CheckS3Data.validateFileEntries(rawFileEntries, fileEntries))
      if (!dryRun) {
        results
          .mapN { (bundle, files) => (bundle, files) }
          .andThen { case (bundle, files) =>
            try {
              //In case something bad happen in the distributed transaction, we store the modification brings to the resource (FHIR and S3 objects)
              writeAheadLog(inputBucket, reportPath, bundle, files)
              CheckS3Data.copyFiles(files, outputBucket)
              val result = bundle.save()
              if (result.isInvalid) {
                CheckS3Data.revert(files, outputBucket)
              }
              result
            } catch {
              case e: Exception =>
                CheckS3Data.revert(files, outputBucket)
                throw e
            }
          }

      } else {
        new Bundle().validNel
      }
    }
  }
}
