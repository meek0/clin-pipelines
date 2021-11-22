package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.fhir.FhirUtils.validateOutcomes
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.fileimport.model.TSpecimen.accessionSystem
import bio.ferlab.clin.etl.task.fileimport.model._
import bio.ferlab.clin.etl.{ValidationResult, isValid}
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

  val CQGC_LAB = "CQGC"

  def validateSpecimen(a: Analysis)(implicit client: IClinFhirClient, fhirClient: IGenericClient): ValidationResult[TSpecimen] = {
    val sp = Option(client.findSpecimenByAccession(new TokenParam(accessionSystem(a.ldm, SpecimenType), a.ldmSpecimenId)))
    validateResource(a, sp, SpecimenType)
  }

  def validateSample(analysis: Analysis)(implicit client: IGenericClient): ValidationResult[TSpecimen] = {
    validateResourceWithParent(analysis, SampleType)
  }

  def validateResourceWithParent(analysis: Analysis, sampleType: SampleAliquotType)(implicit client: IGenericClient): ValidationResult[TSpecimen] = {
    val (id, currentAccessionSystem, parentId) = sampleType match {
      case SampleType => (analysis.ldmSampleId, accessionSystem(analysis.ldm, sampleType), analysis.ldmSpecimenId)
      case AliquotType => (analysis.labAliquotId, accessionSystem(CQGC_LAB, sampleType), analysis.ldmSampleId)
    }

    val results = client
      .search
      .forResource(classOf[Specimen])
      .encodedJson
      .where(ACCESSION.exactly().systemAndCode(currentAccessionSystem, id))
      .include(INCLUDE_PARENT)
      .returnBundle(classOf[Bundle]).execute

    val entries: Seq[Bundle.BundleEntryComponent] = results.getEntry.asScala.toSeq
    val fhirParent = entries.collectFirst { case be if be.getSearch.getMode == SearchEntryMode.INCLUDE => be.getResource.asInstanceOf[Specimen] }
    val fhirSample = entries.collectFirst { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Specimen] }
    val sampleValidation = validateResource(analysis, fhirSample, sampleType)

    val parentAccessionSystem = accessionSystem(analysis.ldm, sampleType.parentType)

    def specimenAccessionEquals(fsp: Specimen) = {
      val identifier = new Identifier().setSystem(parentAccessionSystem).setValue(parentId)
      fsp.getAccessionIdentifier.equalsShallow(identifier)
    }

    val parentValidation: ValidatedNel[String, Any] = (fhirParent, fhirSample) match {
      case (Some(fsp), None) =>
        val (specimenAccessionSystem, specimenAccessionValue) = specimenAccessionSystemAndValue(fsp)
        s"$sampleType $id : A parent specimen id=${fsp.getId}, accession_system=$specimenAccessionSystem, accession_value=$specimenAccessionValue has been returned without children".invalidNel[Any] //should never happened
      case (None, Some(fsa)) =>
        s"$sampleType $id : no parent specimen has been found".invalidNel[Any] //should never happened
      case (Some(fsp), Some(_)) if !specimenAccessionEquals(fsp) =>
        val (specimenAccessionSystem, specimenAccessionValue) = specimenAccessionSystemAndValue(fsp)
        s"$sampleType ${analysis.ldmSampleId} : parent specimen are not the same ($parentId <-> $specimenAccessionValue AND ($parentAccessionSystem <-> $specimenAccessionSystem)".invalidNel[Any]
      case (Some(_), Some(_)) => Valid()
      case (None, None) => Valid()
    }
    sampleValidation.appendErrors(parentValidation)
  }

  private def specimenAccessionSystemAndValue(fsp: Specimen): (String, String) = {
    val specimenAccessionSystem = Option(fsp.getAccessionIdentifier).map(_.getSystem).getOrElse("None")
    val specimenAccessionValue = Option(fsp.getAccessionIdentifier).map(_.getValue).getOrElse("None")
    (specimenAccessionSystem, specimenAccessionValue)
  }

  private def validateResource(a: Analysis, specimen: Option[Specimen], specimenType: SpecimenSampleType)(implicit client: IGenericClient): ValidatedNel[String, TSpecimen] = {
    val (id, stype, ldm) = specimenType match {
      case SpecimenType => (a.ldmSpecimenId, a.specimenType, a.ldm)
      case SampleType => (a.ldmSampleId, a.sampleType.getOrElse(a.specimenType), a.ldm)
      case AliquotType => (a.labAliquotId, a.sampleType.getOrElse(a.specimenType), CQGC_LAB)
    }
    specimen match {
      case None =>
        val s = TNewSpecimen(ldm, id, stype, a.bodySite, specimenType)
        val outcome = s.validateBaseResource
        validateOutcomes(outcome, s) { o =>
          val diag = o.getDiagnostics
          val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
            .replace("Parameters.parameter[0].resource.ofType(Specimen).", "")
            .replace(".coding[0]", "")
          s"Error $specimenType : $loc - $diag"
        }

      case Some(sp) =>
        val potentialErrors = validatePatient(sp, a, id, specimenType) ++ validateSpecimenType(sp, id, stype, specimenType)
        isValid(TExistingSpecimen(sp), potentialErrors)

    }
  }

  private def validatePatient(sp: Specimen, a: Analysis, specimenId: String, label: SpecimenSampleType): Seq[String] = {
    val specimenSubjectId = sp.getSubject.getReference.replace("Patient/", "")
    if (specimenSubjectId != a.patient.clinId) {
      Seq(s"$label id=${specimenId} : does not belong to the same patient (${a.patient.clinId} <-> $specimenSubjectId)")
    } else {
      Nil
    }
  }

  private def validateSpecimenType(sp: Specimen, specimenId: String, specimenType: String, label: SpecimenSampleType): Seq[String] = {
    val existingSpecimenType = Option(sp.getType).map(cc => cc.getCodingFirstRep.getCode)
    if (!existingSpecimenType.contains(specimenType)) {
      Seq(s"$label id=${specimenId} : does not have the same type (${specimenType} <-> ${existingSpecimenType.getOrElse("None")})")
    }
    else {
      Nil
    }
  }


}
