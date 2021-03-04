package bio.ferlab.clin.etl.task.ingestion

import scala.concurrent.duration.DurationInt

object Exporter extends App {

  val Array(tables, keyCloakSecret, storeAccess, storeSecret) = args

  case class Config(url: String, id: String, secret: String)

  val keyCloakConfig = Config("https://auth.qa.cqdg.ferlab.bio/auth/realms/clin/protocol/openid-connect/token", "clin-system", keyCloakSecret)
  val storeConfig = Config("https://esc.calculquebec.ca:8080", storeAccess, storeSecret)
  val fhirExporter = new FhirBulkExporter(
    authConfig = keyCloakConfig,
    fhirUrl= "https://fhir.qa.clin.ferlab.bio/fhir",
    storeConfig = storeConfig
  )

  val url = tables match {
    case "all" => fhirExporter.requestBulkExportFor(FhirBulkExporter.ALL_ENTITIES)
    case entities => fhirExporter.requestBulkExportFor(entities)
  }

  val files = fhirExporter.checkPollingStatus(url, 1.seconds, 100.seconds)
  fhirExporter.uploadFiles(files)

}

