package bio.ferlab.clin.etl.task.validation

import bio.ferlab.clin.etl.model.{Analysis, TDocumentReferences, FileEntry, TDocumentReference}
import bio.ferlab.clin.etl.{ValidationResult, isValid}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits._
import org.hl7.fhir.r4.model.OperationOutcome

import scala.collection.JavaConverters._
object DocumentReferencesValidation {

  def validateFiles(files: Map[String, FileEntry], a: Analysis)(implicit client: IGenericClient): ValidationResult[TDocumentReferences] = {
    def validateOneFile(f: String, label:String) = files.get(f) match {
      case Some(file) =>
        val dr = TDocumentReference(objectStoreId = file.id, title = f, md5 = file.md5, size = file.size)
        val outcome = dr.validateBaseResource()
        val issues = outcome.getIssue.asScala
        val errors = issues.collect {
          case o if o.getSeverity.ordinal() <= OperationOutcome.IssueSeverity.ERROR.ordinal =>
            val diag = o.getDiagnostics
            val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
            s"File type=$label, file_name=${file.name}, specimen=${a.specimenId}, sample=${a.sampleId}, patient:${a.patient.id} : $loc - $diag"
        }.toSeq
        isValid(dr, errors)
      case None => s"File $f does not exist : type:$label, specimen=${a.specimenId}, sample=${a.sampleId}, patient:${a.patient.id}".invalidNel[TDocumentReference]
    }

    (
      validateOneFile(a.files.cram, "cram"),
      validateOneFile(a.files.crai, "crai"),
      validateOneFile(a.files.vcf, "vcf"),
      validateOneFile(a.files.tbi, "tbi"),
      validateOneFile(a.files.qc, "qc")
      ).mapN(TDocumentReferences)

  }
}
