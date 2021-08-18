package bio.ferlab.clin.etl.task.fileimport.model

import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model.Bundle
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime


case class TBundle(resources: List[BundleEntryComponent]) {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  val bundle = new Bundle
  bundle.setType(org.hl7.fhir.r4.model.Bundle.BundleType.TRANSACTION)

  resources.foreach { be =>
    bundle.addEntry(be)
  }

  def save()(implicit client: IGenericClient): Bundle = {
    LOGGER.info("################# Save Bundle ##################")
    val resp = client.transaction.withBundle(bundle).execute
    resp
  }

  def print()(implicit client: IGenericClient): String = {
    client.getFhirContext.newJsonParser.setPrettyPrint(true).encodeResourceToString(bundle)
  }
}
