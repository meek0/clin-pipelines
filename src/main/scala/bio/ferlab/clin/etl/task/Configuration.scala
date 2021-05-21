package bio.ferlab.clin.etl.task

import cats.data.ValidatedNel
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._
import cats.implicits._

case class AWSConf(accessKey: String, secretKey: String, endpoint: String, pathStyleAccess:Boolean)

case class KeycloakConf(realm: String, url: String, clientKey: String, clientSecret: String, audience:String)

case class FhirConf(url: String)

case class FerloadConf(url: String)

case class Conf(aws: AWSConf, keycloak: KeycloakConf, fhir: FhirConf, ferload: FerloadConf)

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
