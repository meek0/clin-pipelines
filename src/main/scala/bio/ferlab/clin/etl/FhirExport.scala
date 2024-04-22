package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.task.fhirexport.FhirBulkExporter
import cats.implicits.catsSyntaxValidatedId

import scala.concurrent.duration.DurationInt

object FhirExport extends App {
  withSystemExit {
    withLog {
      withConf { conf =>
        val Array(tables) = args
        val fhirExporter = new FhirBulkExporter(
          conf.keycloak, conf.fhir.url, conf.aws
        )

        val url = tables match {
          case "all" =>    fhirExporter.requestBulkExportFor(FhirBulkExporter.ALL_ENTITIES)
          case entities => fhirExporter.requestBulkExportFor(entities)
        }

        val files = fhirExporter.checkPollingStatus(url, 1.seconds, 600.seconds)
        val savedFileKeys = fhirExporter.uploadFiles(conf.aws.bucketName, files)
        fhirExporter.cleanUp(conf.aws.bucketName, savedFileKeys).validNel[String]
      }
    }
  }
}
