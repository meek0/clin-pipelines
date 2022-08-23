package bio.ferlab.clin.etl.task.fileimport.model


import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import bio.ferlab.clin.etl.task.fileimport.model.TSpecimen.accessionSystem
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model._

import java.util.Date

trait TSpecimen {
  def buildResource(patientId: Reference, serviceRequest: Reference, organization: Reference, parent: Option[Reference] = None): Either[IdType, Specimen]
}


case class TExistingSpecimen(sp: Specimen) extends TSpecimen with ExistingResource {
  override val resource: Resource = sp
  def buildResource(patientId: Reference, serviceRequest: Reference, organization: Reference, parent: Option[Reference] = None): Either[IdType, Specimen] = Left(id)
}

case class TNewSpecimen(lab: String, submitterId: String, specimenType: String, specimenSampleType: SpecimenSampleType) extends TSpecimen {

  def buildResource(patientId: Reference, serviceRequest: Reference, organization: Reference, parent: Option[Reference] = None): Either[IdType, Specimen] = {
    val specimen: Specimen = buildBase()
    specimen.setId(IdType.newRandomUuid())
    specimen.setSubject(patientId)
    specimen.getRequest.add(serviceRequest)
    specimen.getAccessionIdentifier.setAssigner(organization)
    parent.foreach { r => specimen.getParent.add(r) }
    Right(specimen)

  }

  def validateBaseResource()(implicit fhirClient: IGenericClient): OperationOutcome = {
    val baseResource = buildBase()
    FhirUtils.validateResource(baseResource)
  }

  private def buildBase() = {
    val specimen = new Specimen()
    specimen.setReceivedTimeElement(new DateTimeType(new Date()))
    //    specimen.addIdentifier().setSystem().setValue()
    specimen.getAccessionIdentifier.setSystem(accessionSystem(lab, specimenSampleType)).setValue(submitterId)
    specimen.getType.addCoding()
      .setSystem(CodingSystems.SPECIMEN_TYPE)
      .setCode(specimenType)
    specimen
  }
}

object TSpecimen {

  def accessionSystem(lab: String, specimenSampleType: SpecimenSampleType): String = {
    val labId = lab.replace("Organization/", "")
    s"https://cqgc.qc.ca/labs/$labId/${specimenSampleType.level}"
  }
}
