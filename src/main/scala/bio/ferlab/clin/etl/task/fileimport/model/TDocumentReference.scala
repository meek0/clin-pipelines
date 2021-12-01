package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.{CodingSystems, Extensions}
import bio.ferlab.clin.etl.fhir.FhirUtils.validateOutcomes
import bio.ferlab.clin.etl.task.fileimport.model.TDocumentAttachment.{idFromList, valid}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits._
import org.hl7.fhir.r4.model.DocumentReference.{DocumentReferenceContentComponent, DocumentReferenceContextComponent}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model._

import scala.collection.JavaConverters._

trait TDocumentReference extends DocumentReferenceType {
  def document: Seq[TDocumentAttachment]

  def validateBaseResource()(implicit fhirClient: IGenericClient, ferloadConf: FerloadConf): OperationOutcome = {
    val baseResource = buildBase()
    FhirUtils.validateResource(baseResource)
  }

  def buildResource(subject: Reference, custodian: Reference, related: Seq[Reference])(implicit ferloadConf: FerloadConf): Resource = {
    val dr = buildBase()

    val drc = new DocumentReferenceContextComponent()
    drc.setRelated(related.asJava)
    dr.setContext(drc)

    dr.setId(IdType.newRandomUuid())
    dr.setSubject(subject)
    dr.setCustodian(custodian)
    dr

  }

  private def buildBase()(implicit ferloadConf: FerloadConf) = {
    val dr = new DocumentReference()
    dr.getMasterIdentifier.setSystem(CodingSystems.OBJECT_STORE).setValue(id)
    dr.setStatus(DocumentReferenceStatus.CURRENT)
    dr.getType.addCoding()
      .setSystem(CodingSystems.DR_TYPE)
      .setCode(documentType)
    dr.addCategory().addCoding()
      .setSystem(CodingSystems.DR_CATEGORY)
      .setCode(category)
    val components = document.map { d =>
      val a = new Attachment()
      a.setContentType(d.contentType)
      a.setUrl(s"${ferloadConf.url}/${d.objectStoreId}")
      d.md5.map(md5sum => a.setHash(md5sum.getBytes()))
      a.setTitle(d.title)

      val fullSize = new Extension(Extensions.FULL_SIZE, new DecimalType(d.size))
      a.addExtension(fullSize)

      val drcc = new DocumentReferenceContentComponent(a)
      drcc.getFormat.setSystem(CodingSystems.DR_FORMAT).setCode(d.format)
      drcc
    }

    dr.setContent(components.asJava)
  }
}

object TDocumentReference {
  def validate[T <: TDocumentReference](files: Map[String, FileEntry], a: Analysis)(implicit v: ToReference[T], fhirClient: IGenericClient, ferloadConf: FerloadConf): ValidationResult[T] = v.validate(files, a)
}

trait DocumentReferenceType {
  val documentType: String
  val category: String
  val id: String
}

case class SequencingAlignment(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = SequencingAlignment.documentType
  override val category: String = SequencingAlignment.category
  override val id: String = idFromList[CRAM](document)
}

object SequencingAlignment {
  val documentType: String = "AR"
  val category: String = "SR"
  val label: String = "Sequencing Alignment (CRAM and CRAI)"
  implicit case object builder extends ToReference[SequencingAlignment] {
    override val label: String = SequencingAlignment.label

    protected override def build(documents: Seq[TDocumentAttachment]): SequencingAlignment = SequencingAlignment(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[CRAM], valid[CRAI])

  }
}

case class VariantCalling(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = VariantCalling.documentType
  override val category: String = VariantCalling.category
  override val id: String = idFromList[SNV_VCF](document)
}

object VariantCalling {
  val documentType: String = "SNV"
  val category: String = "SNV"
  val label = "Variant Calling (VCF and TBI)"
  implicit case object builder extends ToReference[VariantCalling] {
    override val label: String = VariantCalling.label

    protected override def build(documents: Seq[TDocumentAttachment]): VariantCalling = VariantCalling(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[SNV_VCF], valid[SNV_TBI])


  }
}

case class CopyNumberVariant(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = CopyNumberVariant.documentType
  override val category: String = CopyNumberVariant.category
  override val id: String = idFromList[CNV_VCF](document)
}

object CopyNumberVariant {
  val documentType: String = "CNV"
  val category: String = "CNV"
  val label = "Copy Number Variant (VCF and TBI)"
  implicit case object builder extends ToReference[CopyNumberVariant] {
    override val label: String = CopyNumberVariant.label

    protected override def build(documents: Seq[TDocumentAttachment]): CopyNumberVariant = CopyNumberVariant(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[CNV_VCF], valid[CNV_TBI])

  }
}

case class QualityControl(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = QualityControl.documentType
  override val category: String = QualityControl.category
  override val id: String = idFromList[QC](document)
}

object QualityControl {
  val documentType: String = "QC"
  val category: String = "RE"
  val label = "QC"
  implicit case object builder extends ToReference[QualityControl] {
    override val label: String = QualityControl.label

    protected override def build(documents: Seq[TDocumentAttachment]): QualityControl = QualityControl(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[QC])
  }
}

trait ToReference[T <: TDocumentReference] {
  def label: String

  protected def build(documents: Seq[TDocumentAttachment]): T

  def attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]]

  def attach(files: Map[String, FileEntry], a: Analysis): Seq[ValidationResult[TDocumentAttachment]] =
    attachments.map(v => v(files, a))

  def validate(files: Map[String, FileEntry], a: Analysis)(implicit client: IGenericClient, ferloadConf: FerloadConf): ValidationResult[T] = {
    attach(files, a).toList.sequence
      .andThen { attachments =>
        val dr: T = build(attachments)
        val outcome = dr.validateBaseResource()
        validateOutcomes(outcome, dr) { o =>
          val diag = o.getDiagnostics
          val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
          s"File type=$label, specimen=${a.ldmSpecimenId}, sample=${a.ldmSampleId}, patient:${a.patient.clinId} : $loc - $diag"
        }
      }

  }


}





