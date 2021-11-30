package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.task.fileimport.model.TDocumentReference.validate
import bio.ferlab.clin.etl.task.fileimport.model._
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits._

object DocumentReferencesValidation {

  def validateFiles(files: Map[String, FileEntry], a: Analysis)(implicit client: IGenericClient, ferloadConf: FerloadConf): ValidationResult[TDocumentReferences] = {

    (
      validate[SequencingAlignment](files, a),
      validate[VariantCalling](files, a),
      validate[CopyNumberVariant](files, a),
      validate[QualityControl](files, a)
      ).mapN(TDocumentReferences)

  }

}
