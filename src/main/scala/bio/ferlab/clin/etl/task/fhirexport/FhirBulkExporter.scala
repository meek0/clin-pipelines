package bio.ferlab.clin.etl.task.fhirexport

import bio.ferlab.clin.etl.conf.{AWSConf, KeycloakConf}
import bio.ferlab.clin.etl.fhir.AuthTokenInterceptor
import bio.ferlab.clin.etl.keycloak.Auth
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.task.fhirexport.FhirBulkExporter._
import bio.ferlab.clin.etl.task.fhirexport.Poller.Task
import bio.ferlab.clin.etl.task.fileimport.model.FileEntry
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Reads._
import play.api.libs.json._
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{DeleteObjectRequest, ListObjectsRequest, PutObjectRequest}
import sttp.client3.{HttpURLConnectionBackend, basicRequest, _}
import sttp.model.StatusCode

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}

object FhirBulkExporter {

  val rawLandingFHIR = "raw/landing/fhir/"

  val ALL_ENTITIES: String =
    Set(
      "ClinicalImpression",
      "Group",
      "Observation",
      "Organization",
      "Patient",
      "Practitioner",
      "PractitionerRole",
      "ServiceRequest",
      "Specimen",
      "Task",
      "DocumentReference",
      "CodeSystem"
    )
      .mkString(",")

  case class ExportOutputFile(`type`: String, url: String)

  case class PollingResponse(transactionTime: String, request: String, output: Seq[ExportOutputFile])

  implicit val exportOutputReads: Reads[ExportOutputFile] = Json.reads[ExportOutputFile]
  implicit val pollingResponseReads: Reads[PollingResponse] = Json.reads[PollingResponse]
}

class FhirBulkExporter(authConfig: KeycloakConf,
                       fhirUrl: String,
                       storeConfig: AWSConf) extends BulkExport {

  val exportUrl: String => String = { entities => s"$fhirUrl/$$export?_type=$entities&_outputFormat=application/ndjson" }
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  val auth: Auth = new AuthTokenInterceptor(authConfig).auth

  override def getAuthentication: String = {
    auth.withToken { (_, rpt) => rpt }
  }

  override def requestBulkExportFor(entities: String): String = {
    val backend = HttpURLConnectionBackend()
    val response =
      basicRequest
        .headers(Map(
          "Authorization" -> s"Bearer $getAuthentication",
          "Prefer" -> "respond-async",
          "Cache-Control" -> "no-cache"
        ))
        .get(uri"${exportUrl(entities)}")
        .send(backend)

    backend.close
    response.code match {
      case StatusCode.Accepted =>
        response.headers.find(_.name == "Content-Location").map(_.value)
          .getOrElse(throw new RuntimeException("Bulk export was accepted by the server but no polling url was found in the header [Content-Location]"))
      case _ => throw new RuntimeException(s"Bulk export returned: ${response.toString}")
    }
  }

  private def checkPollingStatusOnce(pollingUrl: String): Option[List[(String, String)]] = {
    val backend = HttpURLConnectionBackend()
    val response = basicRequest
      .headers(Map(
        "Authorization" -> s"Bearer $getAuthentication"
      ))
      .get(uri"$pollingUrl")
      .send(backend)

    backend.close
    response.code match {
      case StatusCode.Ok =>
        Some(
          (Json.parse(response.body.right.get) \ "output")
            .as[Seq[ExportOutputFile]]
            .map(output => output.`type` -> output.url)
            .toList
        )

      case StatusCode.Accepted =>
        LOGGER.info(s"Export Progress: ${response.headers.find(_.name == "X-Progress").map(_.value).getOrElse("unknown")}")
        None

      case s =>
        LOGGER.info(s"Unexpected status code: $s")
        LOGGER.info(s"With body: ${response.body}")
        None
    }

  }

  override def checkPollingStatus(pollingUrl: String, interval: Duration, timeout: Duration): List[(String, String)] = {
    val pollingTask = Task(pollingUrl, interval, () => {
      checkPollingStatusOnce(pollingUrl)
    }, timeout)
    val pollingResult = Poller.Default.addTask(pollingTask)
    Await.result(pollingResult, timeout + 1.second)
  }

  private def getFileContent(url: String): String = {
    //TODO : strem the response https://sttp.softwaremill.com/en/latest/responses/body.html#streaming
    val backend = HttpURLConnectionBackend()
    val response = basicRequest
      .headers(Map("Authorization" -> s"Bearer $getAuthentication"))
      .get(uri"$url")
      .send(backend)

    backend.close
    response.body.right.get
  }

  override def uploadFiles(bucketName: String, files: List[(String, String)]): Seq[String] = {
    val s3Client: S3Client = buildS3Client(storeConfig)

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp = LocalDateTime.now().format(formatter)

    files.foreach(f=>LOGGER.info(f.toString()))

    var fileKeys : Seq[String] = Seq()

    files
      .groupBy { case (entity, _) => entity }
      .map { case (entity, list) => entity -> list.zipWithIndex }
      .foreach {
        case (_, filesWithIndex) =>
          filesWithIndex.foreach {
            case ((folderName, fileUrl), idx) =>
              val filekey = s"$rawLandingFHIR$folderName/${folderName}_${idx}_$timestamp.json"
              LOGGER.info(s"upload object to: $bucketName/$filekey")
              val putObj = PutObjectRequest.builder().bucket(bucketName).key(filekey).build()
              s3Client.putObject(putObj, RequestBody.fromString(getFileContent(fileUrl)))
              fileKeys = fileKeys ++ Seq(filekey)
          }
      }
    fileKeys
  }

  override def cleanUp(bucketName: String,ignoreKeys: Seq[String]): Unit = {
    val s3Client: S3Client = buildS3Client(storeConfig)

    LOGGER.info(s"Clean up: $bucketName/$rawLandingFHIR")

    val list = ListObjectsRequest.builder().bucket(bucketName).prefix(rawLandingFHIR).build()
    s3Client.listObjects(list).contents().forEach(o => {
      val key = o.key()
      if (!ignoreKeys.contains(key)) {
        LOGGER.info(s"Delete previous file: ${key}")
        val del = DeleteObjectRequest.builder().bucket(bucketName).key(key).build()
        s3Client.deleteObject(del)
      }
    })

  }
}