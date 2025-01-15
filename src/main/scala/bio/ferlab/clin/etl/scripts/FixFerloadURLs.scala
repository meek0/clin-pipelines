package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.hl7.fhir.r4.model.{Bundle, DocumentReference}
import org.slf4j.{Logger, LoggerFactory}

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
          var updated = false
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
              updated = true
            }
          })
          if (updated){
            res = res ++ FhirUtils.bundleUpdate(Seq(doc))
          }
        })
        Thread.sleep(1000L) // don't spam FHIR too much, we have time
        if (!dryRun && res.nonEmpty) {
          val bundle = TBundle(res.toList)
          LOGGER.info("Request:\n" + bundle.print())
          val result = bundle.save()
          if (result.isValid) {
            LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
          }
          fixFerloadURLs(0, size) // restart from the beginning
        } else {
          fixFerloadURLs(offset + size, size)
        }
      }
    }
    fixFerloadURLs()
    Valid(true)
  }


}
