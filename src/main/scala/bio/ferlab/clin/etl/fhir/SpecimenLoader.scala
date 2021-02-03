package bio.ferlab.clin.etl.fhir

import java.util.Date

import bio.ferlab.clin.etl.fhir.Model.{ClinSpecimenType, Terminology}
import org.hl7.fhir.r4.model._

package object SpecimenLoader {

  def loadSpecimens(clinClient: IClinFhirClient): Seq[Specimen] = ???

  def loadSamples(clinClient: IClinFhirClient, specimens: Map[String, Specimen]): Seq[Specimen] = ???

  def createSpecimen(clinClient: IClinFhirClient, organizationId: String, patientId: String, specimenType: ClinSpecimenType.Value, bodySite: Terminology): Specimen = {
    val specimen: Specimen = new Specimen()

    specimen.setId(IdType.newRandomUuid())
    specimen.setReceivedTimeElement(new DateTimeType(new Date()))

    val org: Organization = clinClient.getOrganizationById(new IdType(organizationId));
    specimen.addIdentifier()
      .setSystem("http://terminology.hl7.org/CodeSystem/v2-0203")
      .setValue(IdType.newRandomUuid().getIdPart)
      .setAssigner(new Reference(org))

    val patient: Patient = clinClient.getPatientById(new IdType(patientId))
    specimen.setSubject(new Reference(patient));

    val specimenCC: CodeableConcept = new CodeableConcept()
    val specimenCoding: Coding = new Coding()
    specimenCoding.setSystem("http://terminology.hl7.org/CodeSystem/v2-0487")
    specimenCoding.setCode(specimenType.toString) //BLD | TUMOR
    specimenCoding.setDisplay(specimenType.display) //Whole blood | Tumor
    specimenCC.addCoding(specimenCoding)
    specimenCC.setText(specimenType.text) //Blood | Tumor
    specimen.setType(specimenCC)

    val specimenCollectionComponent: Specimen.SpecimenCollectionComponent = new Specimen.SpecimenCollectionComponent();

    val bodySiteCC: CodeableConcept = new CodeableConcept()
    val bodySiteCoding: Coding = new Coding()
    bodySiteCoding.setSystem(bodySite.system)
    bodySiteCoding.setCode(bodySite.code)
    bodySiteCoding.setDisplay(bodySite.display)
    bodySiteCC.addCoding(bodySiteCoding)
    if (bodySite.text.isDefined)
      bodySiteCC.setText(bodySite.text.get)

    specimenCollectionComponent.setBodySite(bodySiteCC)
    specimen.setCollection(specimenCollectionComponent)
  }
}
