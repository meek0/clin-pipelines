package bio.ferlab.clin.etl.task.fhirexport

import scala.concurrent.duration.Duration

trait BulkExport {

  def getAuthentication: String

  def requestBulkExportFor(entities: String): String

  def checkPollingStatus(pollingUrl: String, interval: Duration, timeout: Duration): List[(String, String)]

  def cleanUp(bucketName: String, ignoreKeys: Seq[String]): Unit

  def uploadFiles(BulkExport: String, files: List[(String, String)]): Seq[String]

}

