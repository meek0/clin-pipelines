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
      "size"
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
        row.size
      )
      pW.write(s"${values.mkString("\t")}\n")
    })
    pW.close()

    val attachmentFile = AttachmentFile(s"${runName}_manifest.tsv", file)
    file.deleteOnExit()
    attachmentFile
  }

  def createMsgBody(): String = {
    """
      |Bonjour,
      |
      |Les échantillons soumis récemment par votre laboratoire au CQGC ont été séquencés. Les variants associés sont disponibles pour l'interprétation dans le portail web QLIN (https://portail.cqgc.hsj.rtss.qc.ca/).
      |
      |L’ensemble des fichiers binaires et annexes relatifs au séquençage sont accessibles via la page « Archives » du portail web QLIN. Nous mettons également à votre disposition l’outil ferload-client de QLIN ainsi qu’un manifeste, disponible en pièce jointe, permettant un téléchargement en lot. Voir les instructions à GitHub - https://github.com/Ferlab-Ste-Justine/ferload-client-cli
      |
      |L'équipe du CQGC
      |""".stripMargin
  }
}
