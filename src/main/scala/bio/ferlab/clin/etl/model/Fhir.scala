package bio.ferlab.clin.etl.model


import ca.uhn.fhir.rest.api.MethodOutcome
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.server.exceptions.PreconditionFailedException
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, BundleEntryRequestComponent}
import org.hl7.fhir.r4.model.DocumentReference.{DocumentReferenceContentComponent, DocumentReferenceContextComponent}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model.{Attachment, Bundle, DateTimeType, DocumentReference, IdType, OperationOutcome, Reference, Resource, ServiceRequest, Specimen}

import java.util.Date
import scala.collection.JavaConverters._
import scala.util.Try

object Fhir {

  def patientId(id: String) = {
    new IdType("Patient", id)
  }

  def serviceRequestId(id: String) = {
    new IdType("ServiceRequest", id)
  }

  def specimenId(id: String) = {
    new IdType("Specimen", id)
  }

  def validateResource(r: Resource)(implicit client: IGenericClient): OperationOutcome = {
    Try(client.validate().resource(r).execute().getOperationOutcome).recover {
      case e: PreconditionFailedException => e.getOperationOutcome
    }.get.asInstanceOf[OperationOutcome]
  }
}






