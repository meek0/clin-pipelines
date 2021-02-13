package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.model.{Analysis, TExistingSpecimen, TNewSpecimen, TSpecimen}
import bio.ferlab.clin.etl.{ValidationResult, allValid}
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.param.TokenParam
import cats.data.Validated.Valid
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.Specimen.{ACCESSION, INCLUDE_PARENT}
import org.hl7.fhir.r4.model.{Bundle, IdType, Specimen}

import scala.collection.JavaConverters._

object SpecimenValidation {
  def validateSpecimen(a: Analysis)(implicit client: IClinFhirClient): ValidationResult[TSpecimen] = {
    val sp = Option(client.findSpecimenByAccession(new TokenParam(s"https://cqgc.qc.ca/labs/${a.ldm}", a.specimenId)))
    validateOneSpecimen(a, sp, SpecimenType)
  }

  def validateSample(analysis: Analysis)(implicit client: IGenericClient): ValidationResult[TSpecimen] = {
    val results = client
      .search
      .forResource(classOf[Specimen])
      .encodedJson
      .where(ACCESSION.exactly().systemAndCode("https://cqgc.qc.ca/labs/CHUSJ", analysis.sampleId))
      .include(INCLUDE_PARENT)
      .returnBundle(classOf[Bundle]).execute

    val entries: Seq[Bundle.BundleEntryComponent] = results.getEntry.asScala.toSeq
    val fhirParent = entries.collectFirst { case be if be.getSearch.getMode == SearchEntryMode.INCLUDE => be.getResource.asInstanceOf[Specimen] }
    val fhirSample = entries.collectFirst { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Specimen] }
    val sampleValidation = validateOneSpecimen(analysis, fhirSample, SampleType)

    val parentValidation: ValidatedNel[String, Any] = (fhirParent, fhirSample) match {
      case (Some(_), None) => throw new IllegalStateException("A sample parent has been returned without parent") //should never happened
      case (None, Some(fsa)) => "error".invalidNel[Any]
      case (Some(fsp), Some(fsa)) if IdType.of(fsp).getIdPart != analysis.specimenId => "error sample does not have same specimen".invalidNel[Any]
      case (_, Some(_)) => Valid()
      case (None, None) => Valid()
    }
    sampleValidation.appendErrors(parentValidation)
  }

  private def validateOneSpecimen(a: Analysis, specimen: Option[Specimen], label: SpecimenSampleType) = specimen match {
    case None => TNewSpecimen(a.ldm, a.specimenId, a.specimenType, a.bodySite).validNel[String]
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
    if (specimenSubjectId != a.patient.id) {
      s"$label id=${a.specimenId} : does not belong to the same patient (${a.patient.id} <-> $specimenSubjectId)".invalidNel
    } else {
      a.validNel
    }
  }

  private def validateSpecimenType(sp: Specimen, a: Analysis, label: SpecimenSampleType): ValidationResult[Analysis] = {
    val specimenType = Option(sp.getType).map(cc => cc.getCodingFirstRep.getCode)
    if (!specimenType.contains(a.specimenType)) {
      s"$label id=${a.specimenId} : does not have the same type (${a.specimenType} <-> ${specimenType.getOrElse("None")})".invalidNel
    }
    else {
      a.validNel
    }
  }


}
