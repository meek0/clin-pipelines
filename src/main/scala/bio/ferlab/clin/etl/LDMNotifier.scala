package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.MailerConf
import bio.ferlab.clin.etl.keycloak.Auth
import bio.ferlab.clin.etl.mail.MailerService.{adjustBccType, makeSmtpMailer}
import bio.ferlab.clin.etl.mail.{EmailParams, MailerService}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksGqlExtractor.{checkIfGqlResponseHasData, fetchTasksFromFhir}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksMessageComposer.{createMetaDataAttachmentFile, createMsgBody}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksTransformer.groupManifestRowsByLdm
import bio.ferlab.clin.etl.task.ldmnotifier.model.{ManifestRow, Task}
import cats.implicits._
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

object LDMNotifier extends App {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def sendEmails(runName: String,
                 mailerConf: MailerConf,
                 group: Map[(String, Seq[String]), Seq[ManifestRow]]
                ): ValidationResult[List[Unit]] = {

    val mailer = new MailerService(makeSmtpMailer(mailerConf))

    group.toList.traverse { case ((_, email), manifestRows) =>
      val toLDM = email
      val blindCC = adjustBccType(mailerConf)

      withExceptions {
        mailer.sendEmail(
          EmailParams(
            to = toLDM,
            from = mailerConf.from,
            bccs = blindCC,
            subject = "Nouvelles données du CQGC",
            bodyText = createMsgBody(),
            attachments = Seq(createMetaDataAttachmentFile(runName, manifestRows))
          ))

        val extraInfoIfAvailable = if (blindCC.isEmpty) "" else s"and ${blindCC.mkString(",")}"
        LOGGER.info(s"email sent to $toLDM $extraInfoIfAvailable")
      }
    }
  }

  withSystemExit({
    withLog {
      withConf { conf =>
        if (args.length == 0) {
          "first argument must be runName".invalidNel[Any]
        }
        val runName = args(0)

        val auth = new Auth(conf.keycloak)

        val tasksE: Either[String, Seq[Task]] = for {
          strResponseBody <- auth.withToken((_, rpt) => fetchTasksFromFhir(conf.fhir.url, rpt, runName).body)
          rawParsedResponse = Json.parse(strResponseBody)
          taskList <- checkIfGqlResponseHasData(rawParsedResponse)
        } yield taskList

        val tasksV: ValidationResult[Seq[Task]] = tasksE.toValidatedNel

        tasksV.flatMap { tasks: Seq[Task] =>
          val ldmsToManifestRows = groupManifestRowsByLdm(conf.clin.url, tasks)
          sendEmails(runName = runName, mailerConf = conf.mailer, group = ldmsToManifestRows)
        }
      }
    }
  })
}
