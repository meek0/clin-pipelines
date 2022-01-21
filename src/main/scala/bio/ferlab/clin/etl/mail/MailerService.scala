package bio.ferlab.clin.etl.mail

import bio.ferlab.clin.etl.conf.MailerConf
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
    mailerClient.send(Email(
      subject = params.subject,
      from = params.from,
      to = Seq(params.to),
      attachments = params.attachments,
      bodyText = Some(params.bodyText),
      bcc = params.bccs
    ))
  }
}

object MailerService {
  val useSSL = true
  val useTLS = true
  val TLSRequired = true

  def makeSmtpMailer(mailerConf: MailerConf) = new SMTPMailer(SMTPConfiguration(
    mailerConf.host,
    mailerConf.port,
    useSSL,
    useTLS,
    TLSRequired,
  ))

  def adjustBccType(mailerConf: MailerConf): Seq[String] =
    if (mailerConf.bcc.isBlank) Seq.empty
    else mailerConf.bcc.split(",").map(_.trim).toSeq
}