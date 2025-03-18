package bio.ferlab.clin.etl.task.ldmnotifier

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.ldmnotifier.model.{Document, ManifestRow, Task}
import org.hl7.fhir.r4.model.{IdType, ServiceRequest}

import java.net.URI
import java.util.Base64

object TasksTransformer {
  private val patientReferencePrefix = "Patient/"
  private val serviceRequestReferencePrefix = "ServiceRequest/"

  private def keepUrlPathOnly(rawUrl: String): String = {
    val uri = new URI(rawUrl)
    uri.getPath
  }

  private def removePrefix(prefix: String, word: String) = word.replace(prefix, "")

  private def decodeBase64HashIfDefined(hash64: Option[String]): Option[String] = {
    if (hash64.isDefined) {
      val decodedBytes = Base64.getDecoder.decode(hash64.get)
      val decoded = decodedBytes.map(_.toChar).mkString
      Some(decoded)
    } else {
      None
    }
  }

  private def makeCqgcLink(clinBaseUrl: String, patientId: String): String =
    if (clinBaseUrl.isBlank) patientId else s"$clinBaseUrl/patient/$patientId"

  private def makeManifestRows(documents: Seq[Document], extra: Map[String, String]): Seq[ManifestRow] = {
    val serviceRequestReference = extra("serviceRequestReference")
    val clinUrl = extra.getOrElse("clinUrl", "")
    documents.flatMap(document => {
      val patientId = removePrefix(patientReferencePrefix, document.patientReference)
      val fileType = document.fileType
      val sampleId = document.sample.sampleId
      document.contentList.map(content =>
        ManifestRow(
          url = keepUrlPathOnly(content.attachment.url),
          fileName = content.attachment.title,
          fileType = fileType,
          fileFormat = content.fileFormat,
          hash = decodeBase64HashIfDefined(content.attachment.hash64),
          ldmSampleId = sampleId,
          patientId = patientId,
          serviceRequestId = removePrefix(serviceRequestReferencePrefix, serviceRequestReference),
          size = content.attachment.size
        )
      )
    })
  }

  def groupManifestRowsByLdm(clinUrl: String, tasks: Seq[Task]): Map[(String, Seq[String]), Seq[ManifestRow]] = {
    tasks.map(task =>
      (
        (task.requester.alias, task.requester.email),
        makeManifestRows(
          task.documents,
          Map("clinUrl" -> clinUrl, "serviceRequestReference" -> task.serviceRequestReference)
        )
      )
    )
      .groupBy(_._1)
      .mapValues(listOfKeysToRows => listOfKeysToRows.flatMap(keysToRows => keysToRows._2))
  }

  def hasStatPriority(tasks: Seq[Task])(implicit client: IClinFhirClient): Boolean = {
    tasks.foreach(task => {
      val serviceRequest = client.getServiceRequestById(new IdType(removePrefix(serviceRequestReferencePrefix, task.serviceRequestReference)))
      if (serviceRequest.getPriority.equals(ServiceRequest.ServiceRequestPriority.STAT)) return true
    })
    false
  }
}
