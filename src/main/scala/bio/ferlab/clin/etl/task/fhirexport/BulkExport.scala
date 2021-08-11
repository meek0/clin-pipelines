package bio.ferlab.clin.etl.task.fhirexport

import scala.concurrent.duration.Duration

trait BulkExport {

  def getAuthentication: String

  def requestBulkExportFor(entities: String): String

  def checkPollingStatus(pollingUrl: String, interval: Duration, timeout: Duration): List[(String, String)]

  def uploadFiles(bucket_name: String, files: List[(String, String)]): Unit

}

