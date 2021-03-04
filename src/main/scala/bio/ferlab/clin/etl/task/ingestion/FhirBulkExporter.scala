package bio.ferlab.clin.etl.task.ingestion

import bio.ferlab.clin.etl.task.ingestion.Exporter.Config
import bio.ferlab.clin.etl.task.ingestion.FhirBulkExporter._
import bio.ferlab.clin.etl.task.ingestion.Poller.Task
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import play.api.libs.json.Reads._
import play.api.libs.json.{Json, _}
import sttp.client3.{HttpURLConnectionBackend, basicRequest, _}
import sttp.model.{MediaType, StatusCode}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}

object FhirBulkExporter {

  val ALL_ENTITIES: String = Set("Organization", "Patient", "Practitioner", "PractitionerRole").mkString(",")

  case class ExportOutputFile(`type`: String, url: String)
  case class PollingResponse(transactionTime: String, request: String, output: Seq[ExportOutputFile])

  implicit val exportOutputReads: Reads[ExportOutputFile] = Json.reads[ExportOutputFile]
  implicit val pollingResponseReads: Reads[PollingResponse] = Json.reads[PollingResponse]
}

class FhirBulkExporter(authConfig: Config,
                       fhirUrl: String,
                       storeConfig: Config) extends BulkExport {

  val exportUrl: String => String = {entities => s"$fhirUrl/$$export?_type=$entities&_outputFormat=application/ndjson"}

  override def getAuthentication: String = {
    val backend = HttpURLConnectionBackend()
    val response = basicRequest
      .contentType(MediaType.ApplicationXWwwFormUrlencoded)
      .body(
        "grant_type" -> "client_credentials",
        "client_id" -> authConfig.id,
        "client_secret" -> authConfig.secret)
      .post(uri"${authConfig.url}")
      .send(backend)

    backend.close

    if (StatusCode.Ok == response.code && response.body.toString.trim.nonEmpty) {
      (Json.parse(response.body.right.get) \ "access_token").as[String]
    } else {
      throw new RuntimeException(s"Failed to obtain access token from Keycloak.\n${response.body.left.get}")
    }
  }

  override def requestBulkExportFor(entities: String): String = {
    val backend = HttpURLConnectionBackend()
    val response = basicRequest
      .headers(Map(
        "Authorization" -> s"Bearer $getAuthentication",
        "Prefer"-> "respond-async"
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
        println(s"Export Progress: ${response.headers.find(_.name == "X-Progress").map(_.value).getOrElse("unknown")}")
        None

      case s =>
        println(s"Unexpected status code: $s")
        println(s"With body: ${response.body}")
        None
    }

  }

  override def checkPollingStatus(pollingUrl: String, interval: Duration, timeout: Duration): List[(String, String)] = {
    val pollingTask = Task(pollingUrl, interval, () => {checkPollingStatusOnce(pollingUrl)}, timeout)
    val pollingResult = Poller.Default.addTask(pollingTask)
    Await.result(pollingResult, timeout + 1.second)
  }

  private def getFileContent(url: String): String = {
    val backend = HttpURLConnectionBackend()
    val response = basicRequest
      .headers(Map("Authorization" -> s"Bearer $getAuthentication"))
      .get(uri"$url")
      .send(backend)

    backend.close
    response.body.right.get
  }

  override def uploadFiles(files: List[(String, String)]): Unit = {

    val  bucketName: String = "clin"

    val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard
      .withPayloadSigningEnabled(false)
      .withChunkedEncodingDisabled(true)
      .withPathStyleAccessEnabled(true)
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(storeConfig.id, storeConfig.secret)))
      .withEndpointConfiguration(new EndpointConfiguration(storeConfig.url, "RegionOne"))
      .build()

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp = LocalDateTime.now().format(formatter)

    files.foreach(println)

    files
      .zipWithIndex
      .foreach {
        case ((folderName, fileUrl), idx) =>
          val filekey = s"raw/$folderName/${folderName}_${idx}_$timestamp.json"
          println(s"upload object to: $bucketName/$filekey")
          s3Client.putObject(bucketName, filekey, getFileContent(fileUrl))
    }
  }
}
