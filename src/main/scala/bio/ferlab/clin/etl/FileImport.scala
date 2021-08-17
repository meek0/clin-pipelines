package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, Metadata, TBundle}
import bio.ferlab.clin.etl.task.fileimport.{BuildBundle, CheckS3Data}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits.catsSyntaxTuple2Semigroupal
import software.amazon.awssdk.services.s3.S3Client

import java.time.LocalDateTime


object FileImport extends App {
  withSystemExit {
    withLog {
      withConf { conf =>
        val (bucket, prefix, bucketDest, prefixDest, dryRun) = args match {
          case Array(b, p, bd, pd) => (b, p, bd, pd, false)
          case Array(b, p, bd, pd, "true") => (b, p, bd, pd, true)
          case Array(b, p, bd, pd, "false") => (b, p, bd, pd, false)
        }
        implicit val s3Client: S3Client = buildS3Client(conf.aws)
        val (clinClient, client) = buildFhirClients(conf.fhir, conf.keycloak)
        withReport(bucket, prefix) { reportPath =>
          run(bucket, prefix, bucketDest, prefixDest, reportPath, dryRun)(s3Client, client, clinClient, conf.ferload)
        }
      }
    }
  }

  def writeAheadLog(inputBucket: String, reportPath: String, bundle: TBundle, files: Seq[FileEntry])(implicit s3: S3Client, client: IGenericClient): Unit = {
    S3Utils.writeContent(inputBucket, s"$reportPath/bundle.json", bundle.print())
    val filesToCSV = files.map(f => s"${f.key},${f.id}").mkString("\n")
    S3Utils.writeContent(inputBucket, s"$reportPath/files.csv", filesToCSV)
  }

  def run(inputBucket: String, inputPrefix: String, outputBucket: String, outputPrefix: String, reportPath: String, dryRun: Boolean)(implicit s3: S3Client, client: IGenericClient, clinFhirClient: IClinFhirClient, ferloadConf: FerloadConf) = {
    val metadata: ValidatedNel[String, Metadata] = Metadata.validateMetadataFile(inputBucket, inputPrefix)
    metadata.andThen { m: Metadata =>
      val rawFileEntries = CheckS3Data.loadRawFileEntries(inputBucket, inputPrefix)
      val fileEntries = CheckS3Data.loadFileEntries(m, rawFileEntries)
      val results = (BuildBundle.validate(m, fileEntries), CheckS3Data.validateFileEntries(rawFileEntries, fileEntries))
      if (!dryRun) {
        results
          .mapN { (bundle, files) =>
            try {
              //In case something bad happen in the distributed transaction, we store the modification brings to the resource (FHIR and S3 objects)
              writeAheadLog(inputBucket, reportPath, bundle, files)

              CheckS3Data.copyFiles(files, outputBucket, outputPrefix)
              bundle.save()
            } catch {
              case e: Exception =>
                CheckS3Data.revert(files, outputBucket, outputPrefix)
                throw e
            }
          }
      } else {
        results
          .mapN { (bundle, files) => (bundle, files) }
      }
    }
  }
}
