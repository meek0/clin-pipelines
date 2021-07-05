package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.fhir.FhirUtils.validateOutcomes
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.fileimport.model.{Analysis, TExistingSpecimen, TNewSpecimen, TSpecimen}
import bio.ferlab.clin.etl.{ValidationResult, allValid}
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.param.TokenParam
import cats.data.Validated.Valid
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.Specimen.{ACCESSION, INCLUDE_PARENT}
import org.hl7.fhir.r4.model.{Bundle, Identifier, Specimen}

import scala.collection.JavaConverters._

object SpecimenValidation {
  def validateSpecimen(a: Analysis)(implicit client: IClinFhirClient, fhirClient: IGenericClient): ValidationResult[TSpecimen] = {
    val sp = Option(client.findSpecimenByAccession(new TokenParam(s"https://cqgc.qc.ca/labs/${a.ldm}", a.ldmSpecimenId)))
    validateOneSpecimen(a, sp, SpecimenType)
  }

  def validateSample(analysis: Analysis)(implicit client: IGenericClient): ValidationResult[TSpecimen] = {
    val accessionSystem = s"https://cqgc.qc.ca/labs/${analysis.ldm}"
    val results = client
      .search
      .forResource(classOf[Specimen])
      .encodedJson
      .where(ACCESSION.exactly().systemAndCode(accessionSystem, analysis.ldmSampleId))
      .include(INCLUDE_PARENT)
      .returnBundle(classOf[Bundle]).execute

    val entries: Seq[Bundle.BundleEntryComponent] = results.getEntry.asScala.toSeq
    val fhirParent = entries.collectFirst { case be if be.getSearch.getMode == SearchEntryMode.INCLUDE => be.getResource.asInstanceOf[Specimen] }
    val fhirSample = entries.collectFirst { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Specimen] }
    val sampleValidation = validateOneSpecimen(analysis, fhirSample, SampleType)

    def specimenAccessionEquals(fsp: Specimen) = {
      val identifier = new Identifier().setSystem(accessionSystem).setValue(analysis.ldmSpecimenId)
      fsp.getAccessionIdentifier.equalsShallow(identifier)
    }

    val parentValidation: ValidatedNel[String, Any] = (fhirParent, fhirSample) match {
      case (Some(fsp), None) =>
        val specimenAccessionSystem = Option(fsp.getAccessionIdentifier).map(_.getSystem).getOrElse("None")
        val specimenAccessionValue = Option(fsp.getAccessionIdentifier).map(_.getValue).getOrElse("None")
        s"Sample ${analysis.ldmSampleId} : A parent specimen id=${fsp.getId}, accession_system=$specimenAccessionSystem, accession_value=$specimenAccessionValue has been returned without sample".invalidNel[Any] //should never happened
      case (None, Some(fsa)) =>
        s"Sample ${analysis.ldmSampleId} : no parent specimen has been found".invalidNel[Any] //should never happened
      case (Some(fsp), Some(_)) if !specimenAccessionEquals(fsp) =>
        val specimenAccessionSystem = Option(fsp.getAccessionIdentifier).map(_.getSystem).getOrElse("None")
        val specimenAccessionValue = Option(fsp.getAccessionIdentifier).map(_.getValue).getOrElse("None")
        s"Sample ${analysis.ldmSampleId} : parent specimen are not the same (${analysis.ldmSpecimenId} <-> ${specimenAccessionValue} AND (${accessionSystem} <-> $specimenAccessionSystem)".invalidNel[Any]
      case (Some(_), Some(_)) => Valid()
      case (None, None) => Valid()
    }
    sampleValidation.appendErrors(parentValidation)
  }

  private def validateOneSpecimen(a: Analysis, specimen: Option[Specimen], label: SpecimenSampleType)(implicit client: IGenericClient): ValidatedNel[String, TSpecimen] = specimen match {
    case None =>
      val id = if (label == SpecimenType) a.ldmSpecimenId else a.ldmSampleId
      val stype = if (label == SpecimenType) a.specimenType else a.sampleType.getOrElse(a.specimenType)
      val s = TNewSpecimen(a.ldm, id, stype, a.bodySite)
      val outcome = s.validateBaseResource
      validateOutcomes(outcome, s) { o =>
        val diag = o.getDiagnostics
        val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
          .replace("Parameters.parameter[0].resource.ofType(Specimen).", "")
          .replace(".coding[0]", "")
        s"Error $label : $loc - $diag"
      }

    case Some(sp) => allValid(
      validatePatient(sp, a, label),
      validateSpecimenType(sp, a, label)
    )(TExistingSpecimen(sp))
  }

  sealed trait SpecimenSampleType {

  }

  case object SpecimenType extends SpecimenSampleType {
    override def toString: String = "Specimen"
  }

  case object SampleType extends SpecimenSampleType {
    override def toString: String = "Sample"
  }

  private def validatePatient(sp: Specimen, a: Analysis, label: SpecimenSampleType): ValidationResult[Analysis] = {
    val specimenSubjectId = sp.getSubject.getReference.replace("Patient/", "")
    if (specimenSubjectId != a.patient.clinId) {
      s"$label id=${a.ldmSpecimenId} : does not belong to the same patient (${a.patient.clinId} <-> $specimenSubjectId)".invalidNel
    } else {
      a.validNel
    }
  }

  private def validateSpecimenType(sp: Specimen, a: Analysis, label: SpecimenSampleType): ValidationResult[Analysis] = {
    val specimenType = Option(sp.getType).map(cc => cc.getCodingFirstRep.getCode)
    if (!specimenType.contains(a.specimenType)) {
      s"$label id=${a.ldmSpecimenId} : does not have the same type (${a.specimenType} <-> ${specimenType.getOrElse("None")})".invalidNel
    }
    else {
      a.validNel
    }
  }


}
