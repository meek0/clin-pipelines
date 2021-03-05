package bio.ferlab.clin.etl.task.validation

import bio.ferlab.clin.etl.model.{Analysis, CRAI, CRAM, Document, FileEntry, QC, TBI, TDocumentReference, TDocumentReferences, VCF}
import bio.ferlab.clin.etl.{ValidationResult, isValid}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits._
import org.hl7.fhir.r4.model.OperationOutcome

import scala.collection.JavaConverters._
object DocumentReferencesValidation {

  def validateFiles(files: Map[String, FileEntry], a: Analysis)(implicit client: IGenericClient): ValidationResult[TDocumentReferences] = {
    def validateOneFile(f: String, documentType:Document) = files.get(f) match {
      case Some(file) =>
        val dr = TDocumentReference(objectStoreId = file.id, title = f, md5 = file.md5, size = file.size, documentType)
        val outcome = dr.validateBaseResource()
        val issues = outcome.getIssue.asScala
        val errors = issues.collect {
          case o if o.getSeverity.ordinal() <= OperationOutcome.IssueSeverity.ERROR.ordinal =>
            val diag = o.getDiagnostics
            val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
            s"File type=${documentType.label}, file_name=${file.filename}, specimen=${a.specimenId}, sample=${a.sampleId}, patient:${a.patient.id} : $loc - $diag"
        }.toSeq
        isValid(dr, errors)
      case None => s"File $f does not exist : type:${documentType.label}, specimen=${a.specimenId}, sample=${a.sampleId}, patient:${a.patient.id}".invalidNel[TDocumentReference]
    }

    (
      validateOneFile(a.files.cram, CRAM),
      validateOneFile(a.files.crai, CRAI),
      validateOneFile(a.files.vcf, VCF),
      validateOneFile(a.files.tbi, TBI),
      validateOneFile(a.files.qc, QC)
      ).mapN(TDocumentReferences)

  }
}
