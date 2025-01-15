package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.{CodingSystems, Extensions}
import bio.ferlab.clin.etl.fhir.FhirUtils.validateOutcomes
import bio.ferlab.clin.etl.task.fileimport.model.TDocumentAttachment.{firstIdFromList, idFromList, valid}
import bio.ferlab.clin.etl.task.fileimport.model.TFullServiceRequest.EXTUM_SCHEMA
import bio.ferlab.clin.etl.task.fileimport.model.VariantCalling.{documentTypeGermline, documentTypeSomatic}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
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
      a.setUrl(s"${ferloadConf.cleanedUrl}/${d.objectStoreId}")
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
  def validate[T <: TDocumentReference](files: Map[String, FileEntry], a: Analysis, schema: Option[String])(implicit v: ToReference[T], fhirClient: IGenericClient, ferloadConf: FerloadConf): ValidationResult[T] = v.validate(files, a, schema)


}

trait DocumentReferenceType {
  val documentType: String
  val category: String = "GENO"
  val id: String
}

case class SequencingAlignment(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = SequencingAlignment.documentType
  override val id: String = idFromList[CRAM](document)
}

object SequencingAlignment {
  val documentType: String = "ALIR"
  val label: String = "Sequencing Alignment (CRAM and CRAI)"
  implicit case object builder extends ToReference[SequencingAlignment] {
    override val label: String = SequencingAlignment.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): SequencingAlignment = SequencingAlignment(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[CRAM], valid[CRAI])

  }
}

case class VariantCalling(document: Seq[TDocumentAttachment], schema: Option[String]) extends TDocumentReference {
  override val documentType: String = if (EXTUM_SCHEMA.equals(schema.orNull)) VariantCalling.documentTypeSomatic else VariantCalling.documentTypeGermline
  override val id: String = idFromList[SNV_VCF](document)
}

object VariantCalling {
  val documentTypeGermline: String = "SNV"
  val documentTypeSomatic: String = "SSNV"
  val label = "Variant Calling (VCF and TBI)"
  implicit case object builder extends ToReference[VariantCalling] {
    override val label: String = VariantCalling.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): VariantCalling = VariantCalling(documents, schema)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[SNV_VCF], valid[SNV_TBI])


  }
}

case class CopyNumberVariant(document: Seq[TDocumentAttachment], schema: Option[String]) extends TDocumentReference {
  override val documentType: String = if (EXTUM_SCHEMA.equals(schema.orNull)) CopyNumberVariant.documentTypeSomatic else CopyNumberVariant.documentTypeGermline
  override val id: String = idFromList[CNV_VCF](document)
}

object CopyNumberVariant {
  val documentTypeGermline: String = "GCNV"
  val documentTypeSomatic: String = "SCNV"
  val label = "Copy Number Variant (VCF and TBI)"
  implicit case object builder extends ToReference[CopyNumberVariant] {
    override val label: String = CopyNumberVariant.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): CopyNumberVariant = CopyNumberVariant(documents, schema)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[CNV_VCF], valid[CNV_TBI])

  }
}

case class StructuralVariant(document: Seq[TDocumentAttachment], schema: Option[String]) extends TDocumentReference {
  override val documentType: String = if (EXTUM_SCHEMA.equals(schema.orNull)) StructuralVariant.documentTypeSomatic else StructuralVariant.documentTypeGermline
  override val id: String = idFromList[SV_VCF](document)
}

object StructuralVariant {
  val documentTypeGermline: String = "GSV"
  val documentTypeSomatic: String = "SSV"
  val label = "Structural Variant (VCF and TBI)"
  implicit case object builder extends ToReference[StructuralVariant] {
    override val label: String = StructuralVariant.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): StructuralVariant = StructuralVariant(documents, schema)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[SV_VCF], valid[SV_TBI])

  }
}

case class SupplementDocument(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = SupplementDocument.documentType
  override val id: String = idFromList[Supplement](document)
}

object SupplementDocument {
  val documentType: String = "SSUP"
  val label = "Supplement"
  implicit case object builder extends ToReference[SupplementDocument] {
    override val label: String = SupplementDocument.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): SupplementDocument = SupplementDocument(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[Supplement])
  }
}

case class Exomiser(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = Exomiser.documentType
  override val id: String = idFromList[EXOMISER_HTML](document)
}

object Exomiser {
  val documentType: String = "EXOMISER"
  val label = "Exomiser (HTML, JSON and TSV)"
  implicit case object builder extends ToReference[Exomiser] {
    override val label: String = Exomiser.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): Exomiser = Exomiser(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[EXOMISER_HTML], valid[EXOMISER_JSON], valid[EXOMISER_VARIANTS_TSV])

  }
}

case class IgvTrack(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = IgvTrack.documentType
  override val id: String = firstIdFromList(document)
}

object IgvTrack {
  val documentType: String = "IGV"
  val label = "Igv track (BW and BED)"
  implicit case object builder extends ToReference[IgvTrack] {
    override val label: String = IgvTrack.label

    protected override def requiredAll: Boolean = false
    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): IgvTrack = IgvTrack(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[SEG_BW], valid[HARD_FILTERED_BAF_BW], valid[ROH_BED], valid[HYPER_EXOME_HG38_BED])

  }
}

case class CnvVisualization(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = CnvVisualization.documentType
  override val id: String = idFromList[CNV_CALLS_PNG](document)
}

object CnvVisualization {
  val documentType: String = "CNVVIS"
  val label = "CNV Visualization"
  implicit case object builder extends ToReference[CnvVisualization] {
    override val label: String = CnvVisualization.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): CnvVisualization = CnvVisualization(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[CNV_CALLS_PNG])

  }
}

case class CoverageByGene(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = CoverageByGene.documentType
  override val id: String = idFromList[COVERAGE_BY_GENE_CSV](document)
}

object CoverageByGene {
  val documentType: String = "COVGENE"
  val label = "Coverage by Gene Report"
  implicit case object builder extends ToReference[CoverageByGene] {
    override val label: String = CoverageByGene.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): CoverageByGene = CoverageByGene(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[COVERAGE_BY_GENE_CSV])

  }
}

case class QcMetrics(document: Seq[TDocumentAttachment]) extends TDocumentReference {
  override val documentType: String = QcMetrics.documentType
  override val id: String = idFromList[QC_METRICS](document)
}

object QcMetrics {
  val documentType: String = "QCRUN"
  val label = "Sequencing Run QC Report"
  implicit case object builder extends ToReference[QcMetrics] {
    override val label: String = QcMetrics.label

    protected override def build(documents: Seq[TDocumentAttachment], schema: Option[String]): QcMetrics = QcMetrics(documents)

    override val attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]] = Seq(valid[QC_METRICS])

  }
}

trait ToReference[T <: TDocumentReference] {
  def label: String

  protected def build(documents: Seq[TDocumentAttachment], schema: Option[String]): T

  protected def requiredAll = true

  def attachments: Seq[(Map[String, FileEntry], Analysis) => ValidationResult[TDocumentAttachment]]

  def attach(files: Map[String, FileEntry], a: Analysis): Seq[ValidationResult[TDocumentAttachment]] =
    attachments.map(v => v(files, a))

  def validate(files: Map[String, FileEntry], a: Analysis, schema: Option[String])(implicit client: IGenericClient, ferloadConf: FerloadConf): ValidationResult[T] = {
    val attachedFiles = attach(files, a).toList
    if (!requiredAll && attachedFiles.exists(_.isValid)) {
      val validAttachments = attachedFiles.collect { case Valid(v) => v } // as opposed to andThen bellow that would fail on the first invalid
      validateAttachments(validAttachments, a, schema)
    } else {
      attachedFiles.sequence.andThen(validAttachments => validateAttachments(validAttachments, a, schema))
    }
  }

  private def validateAttachments(validAttachments: List[TDocumentAttachment], a: Analysis, schema: Option[String])(implicit client: IGenericClient, ferloadConf: FerloadConf): ValidationResult[T] = {
    val dr: T = build(validAttachments, schema)
    val outcome = dr.validateBaseResource()
    validateOutcomes(outcome, dr) { o =>
      val diag = o.getDiagnostics
      val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
      s"File type=$label, specimen=${a.ldmSpecimenId}, sample=${a.ldmSampleId} : $loc - $diag"
    }
  }
}




