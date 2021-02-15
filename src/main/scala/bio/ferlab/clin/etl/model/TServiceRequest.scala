package bio.ferlab.clin.etl.model

import org.hl7.fhir.r4.model.{Reference, ServiceRequest}

case class TServiceRequest(sr: ServiceRequest) {
  def buildResource(specimen: Reference, sampleReference: Reference): ServiceRequest = {
    sr.addSpecimen(specimen).addSpecimen(sampleReference)
  }

}
