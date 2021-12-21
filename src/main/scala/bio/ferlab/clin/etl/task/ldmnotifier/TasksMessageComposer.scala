package bio.ferlab.clin.etl.task.ldmnotifier

import bio.ferlab.clin.etl.task.ldmnotifier.model.ManifestRow
import play.api.libs.mailer.AttachmentFile

import java.io.{File, PrintWriter}

object TasksMessageComposer {
  def createMetaDataAttachmentFile(runName: String, rows: Seq[ManifestRow]): AttachmentFile = {
    val file = File.createTempFile(s"${runName}_manifest", ".tsv")

    val pW = new PrintWriter(file)
    val header = Seq(
      "url",
      "file_name",
      "file_type",
      "file_format",
      "hash",
      "ldm_sample_id",
      "patient_id",
      "service_request_id",
      "cqgc_link"
    )
    pW.write(s"${header.mkString("\t")}\n")
    rows.foreach(row => {
      val values = Seq(
        row.url,
        row.fileName,
        row.fileType,
        row.fileFormat,
        row.hash.getOrElse(""),
        row.ldmSampleId,
        row.patientId,
        row.serviceRequestId,
        row.cqgcLink
      )
      pW.write(s"${values.mkString("\t")}\n")
    })
    pW.close()

    val attachmentFile = AttachmentFile(s"${runName}_manifest.tsv", file)
    file.deleteOnExit()
    attachmentFile
  }

  def createMsgBody(): String = {
    val resourcePath = getClass.getResource("/ldmEmailPreambleText.txt")
    val source = scala.io.Source.fromFile(resourcePath.getPath)
    val preamble = try source.mkString finally source.close()
    preamble
  }
}
