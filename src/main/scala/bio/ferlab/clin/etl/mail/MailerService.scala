package bio.ferlab.clin.etl.mail

import bio.ferlab.clin.etl.conf.MailerConf
import play.api.libs.mailer._

case class EmailParams(
                        to: Seq[String],
                        from: String,
                        bccs: Seq[String],
                        subject: String,
                        bodyText: String,
                        attachments: Seq[AttachmentFile]
                      )

class MailerService (mailerClient: MailerClient) {
  private type MessageId = String

  def sendEmail(params: EmailParams): MessageId = {
    mailerClient.send(Email(
      subject = params.subject,
      from = params.from,
      to = params.to,
      attachments = params.attachments,
      bodyText = Some(params.bodyText),
      bcc = params.bccs
    ))
  }
}

object MailerService {
  def makeSmtpMailer(mailerConf: MailerConf) = new SMTPMailer(SMTPConfiguration(
    mailerConf.host,
    mailerConf.port,
    mailerConf.ssl,
    mailerConf.tls,
    mailerConf.tlsRequired,
  ))

  def adjustBccType(mailerConf: MailerConf): Seq[String] =
    if (mailerConf.bcc.isBlank) Seq.empty
    else mailerConf.bcc.split(",").map(_.trim).toSeq
}