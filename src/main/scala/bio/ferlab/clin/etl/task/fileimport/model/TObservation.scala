package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.{ANALYSIS_REQUEST_CODE, OBSERVATION_CATEGORY, OBSERVATION_CODE, OBSERVATION_INTERPRETATION}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Profiles.OBSERVATION_PROFILE
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits.catsSyntaxValidatedId
import org.hl7.fhir.r4.model.Observation.ObservationStatus
import org.hl7.fhir.r4.model._

import scala.collection.JavaConverters.asScalaBufferConverter

case class TObservation(analysis: FullAnalysis) {
  val id: IdType = IdType.newRandomUuid()

  val obs = new Observation()
  obs.getMeta.addProfile(OBSERVATION_PROFILE)
  obs.setStatus(ObservationStatus.FINAL)
  obs.addCategory(new CodeableConcept(new Coding().setSystem(OBSERVATION_CATEGORY).setCode("exam")))

  obs.setCode(new CodeableConcept(new Coding(OBSERVATION_CODE, "DSTA", "Patient Disease Status")))

  def validateBaseResource()(implicit client: IGenericClient): ValidatedNel[String, TObservation] = {

    status().andThen { s =>
      val outcomes = FhirUtils.validateResource(obs)
      val interpretation = if (analysis.patient.status == "AFF") "POS" else "NEG"
      obs.addInterpretation(new CodeableConcept(new Coding().setSystem(OBSERVATION_INTERPRETATION).setCode(interpretation)))
      obs.setValue(new CodeableConcept(new Coding().setSystem(ANALYSIS_REQUEST_CODE).setCode(analysis.panelCode)))
      FhirUtils.validateOutcomes(outcomes, this) { o =>
        val diag = o.getDiagnostics
        val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
        s"Observation ${analysis.ldmServiceRequestId} : $loc - $diag"
      }
    }
  }

  def status(): ValidatedNel[String, String] = analysis.patient.status match {
    case "AFF" => "POS".validNel
    case "UNF" => "NEG".validNel
    case "UNK" => "IND".validNel
    case _ => s"Patient ${analysis.patient.firstName} ${analysis.patient.lastName} status ${analysis.patient.status} is not one of these values (AFF, UNF, UNK)".invalidNel
  }

  def createResource(patient: Reference): Resource = {
    obs.setId(id)
    obs.setSubject(patient)

  }

}
