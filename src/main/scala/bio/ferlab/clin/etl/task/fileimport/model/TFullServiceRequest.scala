package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.{ANALYSIS_REQUEST_CODE, FAMILY_IDENTIFIER, SEQUENCING_REQUEST_CODE, SR_IDENTIFIER}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Profiles.{ANALYSIS_SERVICE_REQUEST, SEQUENCING_SERVICE_REQUEST}
import bio.ferlab.clin.etl.task.fileimport.model.TFullServiceRequest.{EXTUM_SCHEMA, GERMLINE_SCHEMA, validateWithFakeSubject}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import org.hl7.fhir.r4.model.ServiceRequest.{ServiceRequestIntent, ServiceRequestStatus}
import org.hl7.fhir.r4.model._

import java.util.Date
import scala.collection.JavaConverters.asScalaBufferConverter


object TFullServiceRequest {

  val EXTUM_SCHEMA = "CQGC_Exome_Tumeur_Seul"
  val GERMLINE_SCHEMA = "CQGC_Germline"

  def validateWithFakeSubject(sr:ServiceRequest)(implicit client: IGenericClient): OperationOutcome ={
    sr.setSubject(new Reference("fake"))
    val outcomes = FhirUtils.validateResource(sr)
    sr.setSubject(null)
    outcomes

  }


}

case class TAnalysisServiceRequest(submissionSchema: Option[String], analysis: FullAnalysis) {
  def buildResource(patient: Reference, familyExtensions: Option[Seq[FamilyExtension]], clinicalImpressions:Seq[Reference], ldm:Reference): Resource = {
    sr.setId(id)
    sr.setSubject(patient)
    sr.addPerformer(ldm)
    familyExtensions.foreach { fel =>
      val extensions: Seq[Extension] = fel.map { familyExtension =>
        val ext = new Extension("http://fhir.cqgc.ferlab.bio/StructureDefinition/family-member")
        ext
          .addExtension("parent", new Reference(familyExtension.patientId))
        val coding = new Coding()
        coding.setCode(familyExtension.code)
        coding.setSystem("http://terminology.hl7.org/CodeSystem/v3-RoleCode")
        ext.addExtension("parent-relationship", new CodeableConcept().addCoding(coding))
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
  if (EXTUM_SCHEMA.equals(submissionSchema.orNull)) {
    sr.setCode(new CodeableConcept().addCoding(new Coding().setSystem(ANALYSIS_REQUEST_CODE).setCode("EXTUM")))
  } else {
    sr.setCode(new CodeableConcept().addCoding(new Coding().setSystem(ANALYSIS_REQUEST_CODE).setCode(analysis.panelCode)))
  }

  analysis.patient.familyId.foreach(f => sr.addIdentifier(new Identifier().setSystem(FAMILY_IDENTIFIER).setValue(f)))
  sr.setAuthoredOn(new Date())

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

case class TSequencingServiceRequest(submissionSchema: Option[String], analysis: FullAnalysis) {
  def buildResource(analysisServiceRequest: Option[Reference], patient:Reference, specimen: Reference, sample: Reference, ldm:Reference): Resource = {
    sr.setId(id)
    analysisServiceRequest.foreach(sr.addBasedOn)
    sr.addSpecimen(specimen)
    sr.addPerformer(ldm)
    sr.addSpecimen(sample)
    sr.setSubject(patient)
    sr
  }

  val id: IdType = IdType.newRandomUuid()
  val sr = new ServiceRequest()

  sr.getMeta.addProfile(SEQUENCING_SERVICE_REQUEST)
  sr.setIntent(ServiceRequestIntent.ORDER)
  sr.setStatus(ServiceRequestStatus.COMPLETED)
  sr.getCode.addCoding().setSystem(SR_IDENTIFIER).setCode(analysis.ldmServiceRequestId)
  sr.setCode(new CodeableConcept().addCoding(new Coding().setSystem(ANALYSIS_REQUEST_CODE).setCode(analysis.panelCode)))
  if (GERMLINE_SCHEMA.equals(submissionSchema.orNull)) {
    sr.getCode().addCoding(new Coding().setSystem(SEQUENCING_REQUEST_CODE).setCode("75020"))
  } else if (EXTUM_SCHEMA.equals(submissionSchema.orNull)) {
    sr.getCode().addCoding(new Coding().setSystem(SEQUENCING_REQUEST_CODE).setCode("65241"))
  }

  sr.setAuthoredOn(new Date())

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