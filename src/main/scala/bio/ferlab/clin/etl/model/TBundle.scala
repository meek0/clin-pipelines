package bio.ferlab.clin.etl.model

import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model.Bundle
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent


case class TBundle(resources: List[BundleEntryComponent]) {
  def save()(implicit client: IGenericClient): Bundle = {
    val bundle = new Bundle
    bundle.setType(org.hl7.fhir.r4.model.Bundle.BundleType.TRANSACTION)

    resources.foreach { be =>
      bundle.addEntry(be)
    }
    import ca.uhn.fhir.context.FhirContext
    val ctx = FhirContext.forR4
    println(ctx.newJsonParser.setPrettyPrint(true).encodeResourceToString(bundle))

    val resp = client.transaction.withBundle(bundle).execute
    resp
  }
}
