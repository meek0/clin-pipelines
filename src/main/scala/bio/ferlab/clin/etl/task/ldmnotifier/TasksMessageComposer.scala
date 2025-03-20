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

  def createMsgBody(ldmPram: String, runNameParam: String, hasStatPriority: Boolean, clinUrl: String): String = {
    val validatedRunNameParam = Some(runNameParam).getOrElse("")
    val urlRunNameParam = if (validatedRunNameParam.isBlank()) "" else s"&s=${runNameParam}"
    val runUrl = s"${clinUrl}/prescription/search?tab=requests&ldm=${ldmPram}${urlRunNameParam}"
    val statUrl = s"${runUrl}&priority=stat"
    val runUrlWithStat = if(hasStatPriority) s"""<span style="color: red;">STAT => Une ou plusieurs prescriptions liées à ces échantillons ont la priorité STAT (<a href="$statUrl">$statUrl</a>).</span><br>""" else ""
    s"""
      |Bonjour,<br>
      |<br>
      |Les échantillons soumis récemment par votre laboratoire au CQGC ont été séquencés. Les variants associés sont disponibles pour l'interprétation dans le portail web QLIN (<a href="$runUrl">$runUrl</a>).<br>
      |<br>
      |$runUrlWithStat<br>
      |L’ensemble des fichiers binaires et annexes relatifs au séquençage sont accessibles via la page « Archives » du portail web QLIN. Nous mettons également à votre disposition l’outil ferload-client de QLIN ainsi qu’un manifeste, disponible en pièce jointe, permettant un téléchargement en lot. Voir les instructions à GitHub - <a href="https://github.com/Ferlab-Ste-Justine/ferload-client-cli">https://github.com/Ferlab-Ste-Justine/ferload-client-cli</a><br>
      |<br>
      |L'équipe du CQGC<br>
      |""".stripMargin
  }
}
