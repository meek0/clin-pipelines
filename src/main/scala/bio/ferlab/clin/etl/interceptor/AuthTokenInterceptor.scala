package bio.ferlab.clin.etl.interceptor

import ca.uhn.fhir.rest.client.api.{IClientInterceptor, IHttpRequest, IHttpResponse}
import org.slf4j.{Logger, LoggerFactory}

//var -> because token needs to be updated when it expires
class AuthTokenInterceptor(var token: String) extends IClientInterceptor{

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  override def interceptRequest(theRequest: IHttpRequest): Unit = {
    LOGGER.debug("HTTP request intercepted.  Adding Authorization header.")
    theRequest.addHeader("Authorization", s"Bearer ${token}")
  }

  override def interceptResponse(theResponse: IHttpResponse): Unit = {
    // Nothing to do here for now
  }
}
