package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.SomaticNormalImport.{buildFileEntryID, formatDisplaySpecimen}
import bio.ferlab.clin.etl.{LOGGER, SomaticNormalImport, ValidationResult}
import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.{DR_CATEGORY, DR_FORMAT, DR_TYPE}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Extensions.FULL_SIZE
import bio.ferlab.clin.etl.fhir.{FhirUtils, IClinFhirClient}
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data.attach
import bio.ferlab.clin.etl.task.fileimport.model.TTask.{EXOME_GERMLINE_ANALYSIS, EXTUM_ANALYSIS, SOMATIC_NORMAL}
import bio.ferlab.clin.etl.task.fileimport.model.{Analysis, FileEntry, FullMetadata, IgvTrack, RawFileEntry, TBundle}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import cats.implicits.catsSyntaxValidatedId
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsSuccess, Json}
import software.amazon.awssdk.services.s3.S3Client
import cats.data.Validated.{Invalid, Valid}
import org.apache.commons.lang3.StringUtils
import org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model.{Attachment, Bundle, CodeableConcept, Coding, DecimalType, DocumentReference, IdType, Reference, Task}

import java.util.UUID
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case object FixFerloadURLs {

  val LOGGER: Logger = LoggerFactory.getLogger(FixFerloadURLs.getClass)

  def apply(conf: Conf, params: Array[String])(implicit fhirClient: IGenericClient): ValidationResult[Boolean] = {

    val dryRun = params.contains("--dryrun")

    def fixFerloadURLs(offset: Int = 0, size: Int = 100): Unit = {
      LOGGER.info(s"Fetching Docs from offset $offset with size $size")
      val results = fhirClient.search().forResource(classOf[DocumentReference])
        .offset(offset)
        .count(size)
        .encodedJson()
        .returnBundle(classOf[Bundle]).execute()

      if (results.getEntry.size() > 0) {
        var res: Seq[BundleEntryComponent] = Seq()
        results.getEntry.forEach(entry => {
          val doc = entry.getResource.asInstanceOf[DocumentReference]
          doc.getContent.forEach(c => {
            val attachment = c.getAttachment
            var url = attachment.getUrl
            if (url.startsWith("https:/ferload.")) {
              url = url.replace("https:/ferload.", "https://ferload.")
            }
            if (url.contains(".qc.ca//")) {
              url = url.replace(".qc.ca//", ".qc.ca/")
            }
            if (!attachment.getUrl.equals(url)) {
              LOGGER.info(s"Found broken URL for ${doc.getIdElement.getIdPart} : ${attachment.getUrl} => ${url}")
              attachment.setUrl(url)
              res = res ++ FhirUtils.bundleUpdate(Seq(doc))
            }
          })
        })
        if (!dryRun && res.nonEmpty) {
          val bundle = TBundle(res.toList)
          LOGGER.info("Request:\n" + bundle.print())
          val result = bundle.save()
          if (result.isValid) {
            LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
          }
        }
        Thread.sleep(1000L) // don't spam FHIR too much, we have time
        fixFerloadURLs(offset + size, size)
      }
    }
    fixFerloadURLs()
    Valid(true)
  }


}
