package bio.ferlab.clin.etl.mail

import bio.ferlab.clin.etl.conf.Conf
import play.api.libs.mailer._

import javax.inject.Inject

case class EmailParams(
                        to: String,
                        from: String,
                        bccs: Seq[String],
                        subject: String,
                        bodyText: String,
                        attachments: Seq[AttachmentFile]
                      )

class MailerService @Inject()(mailerClient: MailerClient) {
  private type MessageId = String

  def sendEmail(params: EmailParams): MessageId = {
    val email = Email(
      params.subject,
      params.from,
      Seq(params.to),
      attachments = params.attachments,
      bodyText = Some(params.bodyText),
      bcc = params.bccs
    )
    mailerClient.send(email)
  }
}

object MailerService {
  def makeSmtpMailer(conf: Conf) = new SMTPMailer(SMTPConfiguration(conf.mailer.host, conf.mailer.port))

  def adjustBccType(conf: Conf): Seq[String] =
    if (conf.mailer.bcc.isBlank) Seq.empty
    else conf.mailer.bcc.split(",").map(_.trim).toSeq
}