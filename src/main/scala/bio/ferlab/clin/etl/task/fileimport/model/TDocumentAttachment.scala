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
    files.get(key).map(f => buildFile(f).validNel[String]).getOrElse(s"File $key does not exist : type=$label, specimen=${a.ldmSpecimenId}, sample=${a.ldmSampleId}, patient:${a.patient.clinId}".invalidNel[T])
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

case class VCF(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "VCF"
}

object VCF {
  implicit case object builder extends ToAttachment[VCF] {
    override def label: String = "vcf"

    override def analysisFileName: Analysis => String = a => a.files.snv_vcf

    override def buildFile: FileEntry => VCF = f => VCF(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class TBI(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "TBI"
}

object TBI {
  implicit case object builder extends ToAttachment[TBI] {
    override def label: String = "tbi"

    override def analysisFileName: Analysis => String = a => a.files.snv_tbi

    override def buildFile: FileEntry => TBI = f => TBI(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}

case class QC(objectStoreId: String, title: String, md5: Option[String], size: Long, contentType: String) extends TDocumentAttachment {
  override val format: String = "TGZ"
}

object QC {
  implicit case object builder extends ToAttachment[QC] {
    override def label: String = "qc"

    override def analysisFileName: Analysis => String = a => a.files.qc

    override def buildFile: FileEntry => QC = f => QC(objectStoreId = f.id, title = f.filename, md5 = f.md5, size = f.size, contentType = f.contentType)
  }
}