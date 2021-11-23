package bio.ferlab.clin.etl.task.fileimport.model

import org.hl7.fhir.r4.model.{Reference, ServiceRequest}

import scala.collection.JavaConverters._

case class TServiceRequest(sr: ServiceRequest) {
  def buildResource(specimen: Reference, sampleReference: Reference): Option[ServiceRequest] = {
    val existingSpecimen = sr.getSpecimen.asScala.map(_.getReference)

    val referenceToAdd = Seq(specimen, sampleReference).filterNot(s => existingSpecimen.contains(s.getReference))
    referenceToAdd match {
      case Nil => None
      case l =>
        Some(l.foldLeft(sr) { case (sr, sp) => sr.addSpecimen(sp) })
    }


  }

}
