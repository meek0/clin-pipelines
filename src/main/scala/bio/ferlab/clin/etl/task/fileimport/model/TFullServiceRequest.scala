package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.{ANALYSIS_REQUEST_CODE, FAMILY_IDENTIFIER, SR_IDENTIFIER}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Profiles.{ANALYSIS_SERVICE_REQUEST, SEQUENCING_SERVICE_REQUEST}
import bio.ferlab.clin.etl.task.fileimport.model.TFullServiceRequest.validateWithFakeSubject
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import org.hl7.fhir.r4.model.ServiceRequest.{ServiceRequestIntent, ServiceRequestStatus}
import org.hl7.fhir.r4.model._

import scala.collection.JavaConverters.asScalaBufferConverter


object TFullServiceRequest {

  def validateWithFakeSubject(sr:ServiceRequest)(implicit client: IGenericClient): OperationOutcome ={
    sr.setSubject(new Reference("fake"))
    val outcomes = FhirUtils.validateResource(sr)
    sr.setSubject(null)
    outcomes

  }


}

case class TAnalysisServiceRequest(analysis: FullAnalysis) {
  def buildResource(patient: Reference, familyExtensions: Option[Seq[FamilyExtension]], clinicalImpressions:Seq[Reference]): Resource = {
    sr.setId(id)
    sr.setSubject(patient)
    familyExtensions.foreach { fel =>
      val extensions: Seq[Extension] = fel.map { familyExtension =>
        val ext = new Extension("http://fhir.cqgc.ferlab.bio/StructureDefinition/family-member")
        ext
          .addExtension("parent", new Reference(familyExtension.patientId))
        val coding = new Coding()
        coding.setCode(familyExtension.code)
        coding.setSystem("http://fhir.cqgc.ferlab.bio/CodeSystem/family-relation")
        ext.addExtension("parent-relationship", coding)
        ext
      }
      extensions.foreach(sr.addExtension)
    }
    clinicalImpressions.map(sr.addSupportingInfo)
    sr
  }
  val id: IdType = IdType.newRandomUuid()
  val sr = new ServiceRequest()

  sr.getMeta.addProfile(ANALYSIS_SERVICE_REQUEST)
  sr.setIntent(ServiceRequestIntent.ORDER)
  sr.setStatus(ServiceRequestStatus.ACTIVE)
  sr.setCode(new CodeableConcept().addCoding(new Coding().setSystem(ANALYSIS_REQUEST_CODE).setCode(analysis.panelCode)))

  analysis.patient.familyId.foreach(f => sr.getCode.addCoding().setSystem(FAMILY_IDENTIFIER).setCode(f))

  def validateBaseResource()(implicit client: IGenericClient): ValidatedNel[String, TAnalysisServiceRequest] = {
    val fakeReference = new Reference("fake")
    sr.addSupportingInfo(fakeReference)
    val outcomes = validateWithFakeSubject(sr)
    sr.getSupportingInfo.remove(fakeReference)
    FhirUtils.validateOutcomes(outcomes, this) { o =>
      val diag = o.getDiagnostics
      val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
      s"Analysis Service Request for patient ${analysis.patient.firstName} ${analysis.patient.lastName}  : $loc - $diag"
    }

  }

}

case class TSequencingServiceRequest(analysis: FullAnalysis) {
  def buildResource(analysisServiceRequest: Option[Reference], patient:Reference, specimen: Reference, sample: Reference): Resource = {
    sr.setId(id)
    analysisServiceRequest.foreach(sr.addBasedOn)
    sr.addSpecimen(specimen)
    sr.addSpecimen(sample)
    sr.setSubject(patient)
    sr
  }

  val id: IdType = IdType.newRandomUuid()
  val sr = new ServiceRequest()

  sr.getMeta.addProfile(SEQUENCING_SERVICE_REQUEST)
  sr.setIntent(ServiceRequestIntent.ORDER)
  sr.setStatus(ServiceRequestStatus.ACTIVE)
  sr.getCode.addCoding().setSystem(SR_IDENTIFIER).setCode(analysis.ldmServiceRequestId)
  sr.setCode(new CodeableConcept().addCoding(new Coding().setSystem(ANALYSIS_REQUEST_CODE).setCode(analysis.panelCode)))

  def validateBaseResource()(implicit client: IGenericClient): ValidatedNel[String, TSequencingServiceRequest] = {
    val fakeReference = new Reference("fake")
    sr.addBasedOn(fakeReference)
    val outcomes = validateWithFakeSubject(sr)
    sr.getBasedOn.remove(fakeReference)

    FhirUtils.validateOutcomes(outcomes, this) { o =>
      val diag = o.getDiagnostics
      val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
      s"Sequencing Service Request ${analysis.ldmServiceRequestId} : $loc - $diag"
    }

  }

}