package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.MailerConf
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.keycloak.Auth
import bio.ferlab.clin.etl.mail.MailerService.{adjustBccType, makeSmtpMailer}
import bio.ferlab.clin.etl.mail.{EmailParams, MailerService}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksGqlExtractor.{checkIfGqlResponseHasData, fetchTasksFromFhir}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksMessageComposer.{createMetaDataAttachmentFile, createMsgBody}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksTransformer.{groupManifestRowsByLdm, hasStatPriority}
import bio.ferlab.clin.etl.task.ldmnotifier.model.{ManifestRow, Task}
import cats.implicits._
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

object LDMNotifier extends App {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def sendEmails(batchId: String,
                 runName: String,
                 mailerConf: MailerConf,
                 group: Map[(String, Seq[String]), Seq[ManifestRow]],
                 hasStatPriority: Boolean,
                 clinUrl: String = "https://portail.cqgc.hsj.rtss.qc.ca"
                ): ValidationResult[List[Unit]] = {

    val mailer = new MailerService(makeSmtpMailer(mailerConf))

    group.toList.traverse { case ((alias, email), manifestRows) =>
      val toLDM = email
      val blindCC = adjustBccType(mailerConf)

      withExceptions {
        mailer.sendEmail(
          EmailParams(
            to = toLDM,
            from = mailerConf.from,
            bccs = blindCC,
            subject = "Nouvelles données du CQGC",
            bodyText = createMsgBody(ldmPram = alias, runNameParam = runName, hasStatPriority, clinUrl),
            attachments = Seq(createMetaDataAttachmentFile(batchId, manifestRows))
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
          "first argument must be batchId".invalidNel[Any]
        }
        val batchId = args(0)

        val auth = new Auth(conf.keycloak)

        val tasksE: Either[String, Seq[Task]] = for {
          strResponseBody <- auth.withToken((_, rpt) => fetchTasksFromFhir(conf.fhir.url, rpt, batchId).body)
          rawParsedResponse = Json.parse(strResponseBody)
          taskList <- checkIfGqlResponseHasData(rawParsedResponse)
        } yield taskList

        val tasksV: ValidationResult[Seq[Task]] = tasksE.toValidatedNel

        val (clinClient, client) = buildFhirClients(conf.fhir, conf.keycloak)
        tasksV.flatMap { tasks: Seq[Task] =>
          val ldmsToManifestRows = groupManifestRowsByLdm(conf.clin.url, tasks)
          val statPriority = hasStatPriority(tasks)(clinClient)
          sendEmails(batchId = batchId, runName = tasks.head.runName, mailerConf = conf.mailer, group = ldmsToManifestRows, statPriority, conf.clin.url)
        }
      }
    }
  })
}
