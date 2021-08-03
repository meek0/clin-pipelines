package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import bio.ferlab.clin.etl.fhir.FhirUtils.validateOutcomes
import bio.ferlab.clin.etl.task.fileimport.model.TDocumentAttachment.valid
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
      d.md5.map( md5sum => a.setHash(md5sum.getBytes()))
      a.setTitle(d.title)
//      a.setSizeElement(new UnsignedIntType(d.size))
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
}

case class SequencingAlignment(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = "AR"
  override val category: String = "SR"
}

object SequencingAlignment {
  implicit case object builder extends ToReference[SequencingAlignment] {
    override val label: String = "Sequencing Alignment (CRAM and CRAI)"

    protected override def build(documents: Seq[TDocumentAttachment]): SequencingAlignment = SequencingAlignment(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[CRAM], valid[CRAI])
  }
}

case class VariantCalling(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = "SNV"
  override val category: String = "SNV"
}

object VariantCalling {
  implicit case object builder extends ToReference[VariantCalling] {
    override val label = "Variant Calling (VCF and TBI)"

    protected override def build(documents: Seq[TDocumentAttachment]): VariantCalling = VariantCalling(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[VCF], valid[TBI])
  }
}

case class QualityControl(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = "QC"
  override val category: String = "RE"
}

object QualityControl {
  implicit case object builder extends ToReference[QualityControl] {
    override val label = "QC"

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
        val dr = build(attachments)
        val outcome = dr.validateBaseResource()
        validateOutcomes(outcome, dr) { o =>
          val diag = o.getDiagnostics
          val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
          s"File type=$label, specimen=${a.ldmSpecimenId}, sample=${a.ldmSampleId}, patient:${a.patient.clinId} : $loc - $diag"
        }
      }

  }


}





