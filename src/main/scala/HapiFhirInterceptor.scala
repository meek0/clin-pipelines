import ca.uhn.fhir.rest.client.api.{IClientInterceptor, IHttpRequest, IHttpResponse}
import org.slf4j.{Logger, LoggerFactory}

class HapiFhirInterceptor(var token: String) extends IClientInterceptor{

  val LOGGER: Logger = LoggerFactory.getLogger(GenomicDataImporter.getClass)

  override def interceptRequest(theRequest: IHttpRequest): Unit = {
    LOGGER.info("HAPI FHIR request intercepted.  Adding Authorization header.")
    LOGGER.info(s"HAPI FHIR auth token :\n${token}")
    theRequest.addHeader("Authorization", s"Bearer ${token}")
  }

  override def interceptResponse(theResponse: IHttpResponse): Unit = {
    // Nothing to do here for now
  }
}
