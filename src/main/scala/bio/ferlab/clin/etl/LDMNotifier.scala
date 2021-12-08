package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.keycloak.Auth
import bio.ferlab.clin.etl.mail.MailerService.{adjustBccType, makeSmtpMailer}
import bio.ferlab.clin.etl.mail.{EmailParams, MailerService}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksGqlExtractor.{checkIfGqlResponseHasData, fetchTasksFromFhir}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksMessageComposer.{createMetaDataAttachmentFile, createMsgBody}
import bio.ferlab.clin.etl.task.ldmnotifier.TasksTransformer.{AliasToEmailAddress, AliasToUrlValues, groupAttachmentUrlsByEachOfOwnerAliases, mapLdmAliasToEmailAddress}
import bio.ferlab.clin.etl.task.ldmnotifier.model.Task
import cats.implicits._
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

case class SenderParams(runName: String,
                        conf: Conf,
                        aliasToEmailAddress: AliasToEmailAddress,
                        urlsByAlias: AliasToUrlValues)

object LDMNotifier extends App {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def sendEmails(params: SenderParams): ValidationResult[List[Unit]] = {

    val mailer = new MailerService(makeSmtpMailer(params.conf))

    params.urlsByAlias.toList.traverse { case (ldmAlias, urls) =>
      val toLDM = params.aliasToEmailAddress(ldmAlias)
      val blindCC = adjustBccType(params.conf)

      withExceptions {
        mailer.sendEmail(EmailParams(
          toLDM,
          params.conf.mailer.from,
          adjustBccType(params.conf),
          "Nouvelles données du CQGC",
          createMsgBody(urls),
          Seq(createMetaDataAttachmentFile(params.runName, urls))
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

        tasksV.flatMap { t: Seq[Task] =>
          val aliasToEmailAddress = mapLdmAliasToEmailAddress(t)
          val aliasToUrls = groupAttachmentUrlsByEachOfOwnerAliases(t)
          sendEmails(SenderParams(runName, conf, aliasToEmailAddress, aliasToUrls))
        }
      }
    }
  })
}
