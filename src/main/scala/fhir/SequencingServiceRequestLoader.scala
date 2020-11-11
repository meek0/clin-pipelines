package fhir

import java.util.{Collections, Date}

import fhir.Model.Terminology
import org.hl7.fhir.r4.model._

import scala.util.Try
import scala.collection.JavaConverters._

package object SequencingServiceRequestLoader {

  def load(clinClient: IClinFhirClient, specimens: Seq[Specimen]): Seq[ServiceRequest] = ???

  /*def load(clinClient: IClinFhirClient, specimens: Seq[Specimen]): Seq[ServiceRequest] = {
    //TODO: Read files from s3 ObjectStore and create specimens
    val code: Terminology = new Terminology("http://snomed.info/sct", "108252007", "Laboratory procedure", None)
    val nanuqRequest: ServiceRequest = createSequencingServiceRequest(clinClient, "PR00101", "957", specimens, "routine", "completed", "order", code)

    Seq(nanuqRequest)
  }*/

  def createSequencingServiceRequest(clinClient: IClinFhirClient, practitionerId: String, patientId: String, specimens: Seq[Specimen], priority: String, status: String, intent: String, code: Terminology): ServiceRequest = {
    val serviceRequest: ServiceRequest = new ServiceRequest()

    serviceRequest.setId(IdType.newRandomUuid())
    serviceRequest.setAuthoredOn(new Date())

    serviceRequest.setStatus(
      Try(ServiceRequest.ServiceRequestStatus.fromCode(status)).getOrElse(ServiceRequest.ServiceRequestStatus.UNKNOWN)
    )

    serviceRequest.setIntent(
      Try(ServiceRequest.ServiceRequestIntent.fromCode(intent)).getOrElse(ServiceRequest.ServiceRequestIntent.NULL)
    )

    serviceRequest.setPriority(
      Try(ServiceRequest.ServiceRequestPriority.fromCode(priority)).getOrElse(ServiceRequest.ServiceRequestPriority.NULL)
    )

    val srCC: CodeableConcept = new CodeableConcept()
    val srCoding: Coding = new Coding()
    srCoding.setSystem(code.system)
    srCoding.setCode(code.toString)
    srCoding.setDisplay(code.display)
    srCC.addCoding(srCoding)

    serviceRequest.setCode(srCC)

    val practitioner: Practitioner = clinClient.getPractitionerById(new IdType(practitionerId));
    serviceRequest.setRequester(new Reference(practitioner))

    val patient: Patient = clinClient.getPatientById(new IdType(patientId))
    serviceRequest.setSubject(new Reference(patient));

    //Associate all specimen for the current patient id
    val specimenReferences: Seq[Reference] = specimens.collect{
      case specimen if patientId.equalsIgnoreCase(specimen.getSubject.getId) => new Reference(specimen)
    }
    serviceRequest.setSpecimen(seqAsJavaList(specimenReferences))
  }
}
