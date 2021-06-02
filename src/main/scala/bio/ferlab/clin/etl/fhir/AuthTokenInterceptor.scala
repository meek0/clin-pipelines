package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.conf.KeycloakConf
import bio.ferlab.clin.etl.keycloak.Auth
import ca.uhn.fhir.rest.client.api.{IClientInterceptor, IHttpRequest, IHttpResponse}
import org.slf4j.{Logger, LoggerFactory}

class AuthTokenInterceptor(conf: KeycloakConf) extends IClientInterceptor {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  val auth = new Auth(conf)

  override def interceptRequest(theRequest: IHttpRequest): Unit = auth.withToken { token =>
    LOGGER.debug("HTTP request intercepted.  Adding Authorization header.")
    theRequest.addHeader("Authorization", s"Bearer $token")
  }

  override def interceptResponse(theResponse: IHttpResponse): Unit = {
    // Nothing to do here for now
  }
}
