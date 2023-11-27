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

  def validateFiles(files: Map[String, FileEntry], a: Analysis, schema: Option[String])(implicit client: IGenericClient, ferloadConf: FerloadConf): ValidationResult[TDocumentReferences] = {
    (
      validate[SequencingAlignment](files, a, schema),
      validate[VariantCalling](files, a, schema),
      validate[CopyNumberVariant](files, a, schema),
      checkOptionalValidation(validate[StructuralVariant](files, a, schema)),
      validate[SupplementDocument](files, a, schema),
      checkOptionalValidation(validate[Exomiser](files, a, schema)),
      checkOptionalValidation(validate[IgvTrack](files, a, schema)),
      checkOptionalValidation(validate[CnvVisualization](files, a, schema)),
      checkOptionalValidation(validate[CoverageByGene](files, a, schema)),
      checkOptionalValidation(validate[QcMetrics](files, a, schema)),
    ).mapN(TDocumentReferences)
  }

  def checkOptionalValidation[T <: TDocumentReference](validated: ValidationResult[T]): ValidatedNel[String, Option[T]] = {
    val result = validated.toOption
    if (validated.isInvalid){
      validated match {
        case Invalid(NonEmptyList(h, t)) => {
          if (h.contains("OPTIONAL")) {
            result.valid  // file not included in the metadata
          } else {
            h.invalidNel  // file in metadata but missing in store
          }
        }
      }
    } else {
      result.valid
    }
  }
}