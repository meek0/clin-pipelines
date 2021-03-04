package bio.ferlab.clin.etl.task.ingestion

import scala.concurrent.duration.Duration

trait BulkExport {

  def getAuthentication: String

  def requestBulkExportFor(entities: String): String

  def checkPollingStatus(pollingUrl: String, interval: Duration, timeout: Duration): List[(String, String)]

  def uploadFiles(files: List[(String, String)]): Unit

}

