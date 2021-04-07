package bio.ferlab.clin.etl.task.validation

import bio.ferlab.clin.etl.model._
import bio.ferlab.clin.etl.{ValidationResult, isValid}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.OperationOutcome

import scala.collection.JavaConverters._

object DocumentReferencesValidation {

  def validateFiles(files: Map[String, FileEntry], a: Analysis)(implicit client: IGenericClient): ValidationResult[TDocumentReferences] = {
    (
      ToSequencingAlignment.validate(files, a),
      ToVariantCalling.validate(files, a),
      ToQCDocumentReference.validate(files, a)
      ).mapN(TDocumentReferences)

  }


  trait ToDocumentAttachment {
    def label: String

    def analysisFileName: Analysis => String

    def buildFile: FileEntry => TDocumentAttachment

    def validateFile(files: Map[String, FileEntry], a: Analysis): ValidatedNel[String, TDocumentAttachment] = {
      val key = analysisFileName(a)
      files.get(key).map(f => buildFile(f).validNel[String]).getOrElse(s"File $key does not exist : type=$label, specimen=${a.specimenId}, sample=${a.sampleId}, patient:${a.patient.id}".invalidNel[TDocumentAttachment])
    }
  }

  case object ToCram extends ToDocumentAttachment {
    override def label: String = "cram"

    override def analysisFileName: Analysis => String = a => a.files.cram

    override def buildFile: FileEntry => TDocumentAttachment = f => CRAM(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size)
  }

  case object ToCrai extends ToDocumentAttachment {
    override def label: String = "crai"

    override def analysisFileName: Analysis => String = a => a.files.crai

    override def buildFile: FileEntry => TDocumentAttachment = f => CRAI(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size)
  }

  case object ToVCF extends ToDocumentAttachment {
    override def label: String = "vcf"

    override def analysisFileName: Analysis => String = a => a.files.vcf

    override def buildFile: FileEntry => TDocumentAttachment = f => VCF(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size)
  }

  case object ToTBI extends ToDocumentAttachment {
    override def label: String = "tbi"

    override def analysisFileName: Analysis => String = a => a.files.tbi

    override def buildFile: FileEntry => TDocumentAttachment = f => TBI(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size)
  }

  case object ToQCAttachment extends ToDocumentAttachment {
    override def label: String = "qc"

    override def analysisFileName: Analysis => String = a => a.files.qc

    override def buildFile: FileEntry => TDocumentAttachment = f => QC(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size)
  }


  trait ToDocumentReference {
    def label: String

    def documentReferenceType: DocumentReferenceType

    def analysisToDocument: Seq[ToDocumentAttachment]

    def validate(files: Map[String, FileEntry], a: Analysis)(implicit client: IGenericClient): ValidationResult[TDocumentReference] = {
      analysisToDocument.toList.map(_.validateFile(files, a)).sequence
        .andThen { attachments =>
          val dr = TDocumentReference(attachments, documentReferenceType)
          val outcome = dr.validateBaseResource()
          val issues = outcome.getIssue.asScala
          val errors = issues.collect {
            case o if o.getSeverity.ordinal() <= OperationOutcome.IssueSeverity.ERROR.ordinal =>
              val diag = o.getDiagnostics
              val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
              s"File type=$label, specimen=${a.specimenId}, sample=${a.sampleId}, patient:${a.patient.id} : $loc - $diag"
          }
          isValid(dr, errors)
        }

    }
  }

  case object ToSequencingAlignment extends ToDocumentReference {
    val label = "Sequencing Alignment (CRAM and CRAI)"

    override def documentReferenceType: DocumentReferenceType = SequencingAlignment

    override def analysisToDocument: Seq[ToDocumentAttachment] = Seq(ToCram, ToCrai)
  }

  case object ToVariantCalling extends ToDocumentReference {
    val label = "Variant Calling (VCF and TBI)"

    override def documentReferenceType: DocumentReferenceType = VariantCalling

    override def analysisToDocument: Seq[ToDocumentAttachment] = Seq(ToVCF, ToTBI)
  }

  case object ToQCDocumentReference extends ToDocumentReference {
    val label = "QC"

    override def documentReferenceType: DocumentReferenceType = QualityControl

    override def analysisToDocument: Seq[ToDocumentAttachment] = Seq(ToQCAttachment)
  }

}

