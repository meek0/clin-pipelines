package bio.ferlab.clin.etl.task.fileimport.model

import cats.data.ValidatedNel
import cats.implicits._

import scala.reflect.macros.Attachments

trait TDocumentAttachment {
  val format: String
  val objectStoreId: String
  val title: String
  val md5: Option[String]
  val size: Long
  val contentType: String
}

object TDocumentAttachment {
  def valid[T <: TDocumentAttachment](files: Map[String, FileEntry], a: Analysis)(implicit toAttachment: ToAttachment[T]): ValidatedNel[String, T] = toAttachment.validateFile(files, a)

  def idFromList[T <: TDocumentAttachment : Manifest](attachments: Seq[TDocumentAttachment]): String = attachments.collectFirst { case a: T => a.objectStoreId }.get
}

trait ToAttachment[T <: TDocumentAttachment] {
  def label: String

  def analysisFileName: Analysis => String

  def buildFile: FileEntry => T

  def validateFile(files: Map[String, FileEntry], a: Analysis): ValidatedNel[String, T] = {
    val key = analysisFileName(a)
    files.get(key).map(f => buildFile(f).validNel[String]).getOrElse(s"File $key does not exist : type=$label, specimen=${a.ldmSpecimenId}, sample=${a.ldmSampleId}".invalidNel[T])
  }
}

case class CRAM(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "CRAM"
}

case object CRAM {
  implicit case object builder extends ToAttachment[CRAM] {
    override def label: String = "cram"

    override def analysisFileName: Analysis => String = a => a.files.cram

    override def buildFile: FileEntry => CRAM = f => CRAM(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class CRAI(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "CRAI"
}

object CRAI {
  implicit case object builder extends ToAttachment[CRAI] {
    override def label: String = "crai"

    override def analysisFileName: Analysis => String = a => a.files.crai

    override def buildFile: FileEntry => CRAI = f => CRAI(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class SNV_VCF(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "VCF"
}

object SNV_VCF {
  implicit case object builder extends ToAttachment[SNV_VCF] {
    override def label: String = "vcf"

    override def analysisFileName: Analysis => String = a => a.files.snv_vcf

    override def buildFile: FileEntry => SNV_VCF = f => SNV_VCF(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class SNV_TBI(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "TBI"
}

object SNV_TBI {
  implicit case object builder extends ToAttachment[SNV_TBI] {
    override def label: String = "tbi"

    override def analysisFileName: Analysis => String = a => a.files.snv_tbi

    override def buildFile: FileEntry => SNV_TBI = f => SNV_TBI(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class CNV_VCF(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "VCF"
}

object CNV_VCF {
  implicit case object builder extends ToAttachment[CNV_VCF] {
    override def label: String = "vcf"

    override def analysisFileName: Analysis => String = a => a.files.cnv_vcf

    override def buildFile: FileEntry => CNV_VCF = f => CNV_VCF(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class CNV_TBI(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "TBI"
}

object CNV_TBI {
  implicit case object builder extends ToAttachment[CNV_TBI] {
    override def label: String = "tbi"

    override def analysisFileName: Analysis => String = a => a.files.cnv_tbi

    override def buildFile: FileEntry => CNV_TBI = f => CNV_TBI(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class SV_VCF(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "VCF"
}

object SV_VCF {
  implicit case object builder extends ToAttachment[SV_VCF] {
    override def label: String = "vcf"

    override def analysisFileName: Analysis => String = a => a.files.sv_vcf

    override def buildFile: FileEntry => SV_VCF = f => SV_VCF(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class SV_TBI(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "TBI"
}

object SV_TBI {
  implicit case object builder extends ToAttachment[SV_TBI] {
    override def label: String = "tbi"

    override def analysisFileName: Analysis => String = a => a.files.sv_tbi

    override def buildFile: FileEntry => SV_TBI = f => SV_TBI(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class Supplement(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "TGZ"
}

object Supplement {
  implicit case object builder extends ToAttachment[Supplement] {
    override def label: String = "supplement"

    override def analysisFileName: Analysis => String = a => a.files.supplement

    override def buildFile: FileEntry => Supplement = f => Supplement(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class EXOMISER_HTML(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "HTML"
}

object EXOMISER_HTML {
  implicit case object builder extends ToAttachment[EXOMISER_HTML] {
    override def label: String = "html"

    override def analysisFileName: Analysis => String = a => a.files.exomiser_html

    override def buildFile: FileEntry => EXOMISER_HTML = f => EXOMISER_HTML(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class EXOMISER_JSON(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "JSON"
}

object EXOMISER_JSON {
  implicit case object builder extends ToAttachment[EXOMISER_JSON] {
    override def label: String = "json"

    override def analysisFileName: Analysis => String = a => a.files.exomiser_json

    override def buildFile: FileEntry => EXOMISER_JSON = f => EXOMISER_JSON(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class EXOMISER_VARIANTS_TSV(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "TSV"
}

object EXOMISER_VARIANTS_TSV {
  implicit case object builder extends ToAttachment[EXOMISER_VARIANTS_TSV] {
    override def label: String = "tsv"

    override def analysisFileName: Analysis => String = a => a.files.exomiser_variants_tsv

    override def buildFile: FileEntry => EXOMISER_VARIANTS_TSV = f => EXOMISER_VARIANTS_TSV(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class SEG_BW(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "BW"
}

object SEG_BW {
  implicit case object builder extends ToAttachment[SEG_BW] {
    override def label: String = "bw"

    override def analysisFileName: Analysis => String = a => a.files.seg_bw

    override def buildFile: FileEntry => SEG_BW = f => SEG_BW(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class HARD_FILTERED_BAF_BW(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "BW"
}

object HARD_FILTERED_BAF_BW {
  implicit case object builder extends ToAttachment[HARD_FILTERED_BAF_BW] {
    override def label: String = "bw"

    override def analysisFileName: Analysis => String = a => a.files.hard_filtered_baf_bw

    override def buildFile: FileEntry => HARD_FILTERED_BAF_BW = f => HARD_FILTERED_BAF_BW(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class ROH_BED(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "BED"
}

object ROH_BED {
  implicit case object builder extends ToAttachment[ROH_BED] {
    override def label: String = "bed"

    override def analysisFileName: Analysis => String = a => a.files.roh_bed

    override def buildFile: FileEntry => ROH_BED = f => ROH_BED(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class HYPER_EXOME_HG38_BED(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "BED"
}

object HYPER_EXOME_HG38_BED {
  implicit case object builder extends ToAttachment[HYPER_EXOME_HG38_BED] {
    override def label: String = "bed"

    override def analysisFileName: Analysis => String = a => a.files.hyper_exome_hg38_bed

    override def buildFile: FileEntry => HYPER_EXOME_HG38_BED = f => HYPER_EXOME_HG38_BED(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class CNV_CALLS_PNG(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "PNG"
}

object CNV_CALLS_PNG {
  implicit case object builder extends ToAttachment[CNV_CALLS_PNG] {
    override def label: String = "png"

    override def analysisFileName: Analysis => String = a => a.files.cnv_calls_png

    override def buildFile: FileEntry => CNV_CALLS_PNG = f => CNV_CALLS_PNG(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class COVERAGE_BY_GENE_CSV(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "CSV"
}

object COVERAGE_BY_GENE_CSV {
  implicit case object builder extends ToAttachment[COVERAGE_BY_GENE_CSV] {
    override def label: String = "csv"

    override def analysisFileName: Analysis => String = a => a.files.coverage_by_gene_csv

    override def buildFile: FileEntry => COVERAGE_BY_GENE_CSV = f => COVERAGE_BY_GENE_CSV(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class QC_METRICS(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "JSON"
}

object QC_METRICS {
  implicit case object builder extends ToAttachment[QC_METRICS] {
    override def label: String = "json"

    override def analysisFileName: Analysis => String = a => a.files.qc_metrics

    override def buildFile: FileEntry => QC_METRICS = f => QC_METRICS(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}
