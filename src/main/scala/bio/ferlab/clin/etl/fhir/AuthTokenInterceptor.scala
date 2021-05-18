package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.keycloak.Auth
import bio.ferlab.clin.etl.keycloak.Auth.withToken
import ca.uhn.fhir.rest.client.api.{IClientInterceptor, IHttpRequest, IHttpResponse}
import org.slf4j.{Logger, LoggerFactory}

class AuthTokenInterceptor extends IClientInterceptor {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  override def interceptRequest(theRequest: IHttpRequest): Unit = withToken { token =>
    LOGGER.debug("HTTP request intercepted.  Adding Authorization header.")
    theRequest.addHeader("Authorization", s"Bearer $token")
  }

  override def interceptResponse(theResponse: IHttpResponse): Unit = {
    // Nothing to do here for now
  }
}
