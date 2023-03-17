package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.task.fileimport.model.TDocumentReference.validate
import bio.ferlab.clin.etl.task.fileimport.model._
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Invalid
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._

object DocumentReferencesValidation {

  def validateFiles(legacy: Boolean, files: Map[String, FileEntry], a: Analysis)(implicit client: IGenericClient, ferloadConf: FerloadConf): ValidationResult[TDocumentReferences] = {
    (
      validate[SequencingAlignment](files, a),
      validate[VariantCalling](files, a),
      validate[CopyNumberVariant](files, a),
      checkOptionalValidation(legacy, validate[StructuralVariant](files, a)),
      validate[SupplementDocument](files, a),
      checkOptionalValidation(legacy, validate[Exomiser](files, a)),
      checkOptionalValidation(legacy, validate[IgvTrack](files, a)),
      checkOptionalValidation(legacy, validate[CnvVisualization](files, a)),
      checkOptionalValidation(legacy, validate[CoverageByGene](files, a)),
      checkOptionalValidation(legacy, validate[QcMetrics](files, a)),
    ).mapN(TDocumentReferences)
  }

  // convert to optional or return the invalid message if not legacy mode
  def checkOptionalValidation[T <: TDocumentReference](legacy: Boolean, validated: ValidationResult[T]): ValidatedNel[String, Option[T]] = {
    val result = validated.toOption
    if (!legacy && validated.isInvalid){
      validated match {
        case Invalid(NonEmptyList(h, t)) => h.invalidNel
      }
    } else {
      result.valid
    }
  }
}