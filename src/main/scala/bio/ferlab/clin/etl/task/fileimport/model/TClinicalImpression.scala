package bio.ferlab.clin.etl.task.fileimport.model

import org.hl7.fhir.r4.model.ClinicalImpression.{ClinicalImpressionInvestigationComponent, ClinicalImpressionStatus}
import org.hl7.fhir.r4.model.{ClinicalImpression, CodeableConcept, IdType, Reference, Resource}

case class TClinicalImpression(analysis:FullAnalysis) {
  val id:IdType = IdType.newRandomUuid()

  val ci = new ClinicalImpression()
  ci.setStatus(ClinicalImpressionStatus.COMPLETED)

  def createResource(patient:Reference, diseaseStatus:Reference):Resource ={
    ci.setId(id)
    ci.setSubject(patient)
    val component = new ClinicalImpressionInvestigationComponent(new CodeableConcept().setText("Examination / signs"))
    component.addItem(diseaseStatus)
    ci.addInvestigation(component)
  }

}
