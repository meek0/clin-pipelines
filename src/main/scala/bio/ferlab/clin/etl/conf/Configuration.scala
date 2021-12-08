package bio.ferlab.clin.etl.conf

import cats.data.ValidatedNel
import cats.implicits._
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._

case class AWSConf(accessKey: String, secretKey: String, endpoint: String, pathStyleAccess: Boolean, bucketName: String, outputBucketName: String, outputPrefix: String)

case class KeycloakConf(realm: String, url: String, clientKey: String, clientSecret: String, audience: String)

case class FhirConf(url: String)

case class FerloadConf(url: String)

case class MailerConf(host: String, port: Int, from: String, bcc: String)

case class Conf(aws: AWSConf, keycloak: KeycloakConf, fhir: FhirConf, ferload: FerloadConf, mailer: MailerConf)

object Conf {

  def readConf(): ValidatedNel[String, Conf] = {
    val confResult: Result[Conf] = ConfigSource.default.load[Conf]
    confResult match {
      case Left(errors) =>
        val message = errors.prettyPrint()
        message.invalidNel[Conf]
      case Right(conf) => conf.validNel[String]

    }
  }
}
