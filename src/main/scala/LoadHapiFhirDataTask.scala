import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import ca.uhn.fhir.rest.server.exceptions.AuthenticationException
import com.google.gson.{JsonObject, JsonParser}
import org.hl7.fhir.r4.model.{Bundle, Patient}
import org.slf4j.{Logger, LoggerFactory}
import sttp.client3.{HttpURLConnectionBackend, _}
import sttp.model.{MediaType, StatusCode}


object LoadHapiFhirDataTask {
  val LOGGER: Logger = LoggerFactory.getLogger(GenomicDataImporter.getClass)

  val authRequestMaxRetries: Int = 3

  val fhirContext: FhirContext = FhirContext.forR4()
  fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
  fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)

  def run(hapiFhirServerBase: String): Unit = {
    // The creation of the client is an expensive operation so, make sure not to create
    // a new client for each request.
    val client:IGenericClient = fhirContext.newRestfulGenericClient(hapiFhirServerBase)
    val authToken: String = getAuthToken()

    val hapiFhirInterceptor: HapiFhirInterceptor = new HapiFhirInterceptor(authToken)
    client.registerInterceptor(hapiFhirInterceptor)

    createSpecimens(client, hapiFhirInterceptor)

  }

  private def createSpecimens(client: IGenericClient, interceptor: HapiFhirInterceptor): Unit = {
    /**
    *  TODO: Loop on specimens or batches of specimens...
    **/
    for( x <- 1 to 3){ // Simulate looping on specimens
      try{
        retryOnAuthTokenExpired(authRequestMaxRetries, interceptor){
          val results:Bundle = client.search
            .forResource(classOf[Patient])
            .where(Patient.FAMILY.matches.value("Bambois"))
            .returnBundle(classOf[Bundle])
            .execute

          LOGGER.info(s"Found ${results.getEntry.size} patients named 'Bambois'")
        }
      } catch {
        case e: Throwable => {
          // TODO: Determine how to handle a failure to save a specimen or batch of specimens in Fhir
          // TODO: Log the error and stop or continue processing the rest of the specimens?  Rollback?
          LOGGER.error(s"An error occured tyring to save specimen in HAPI Fhir.", e)
          throw e
        }
      }
    }
  }

  def retryOnAuthTokenExpired[T](maxAttempts: Int, interceptor: HapiFhirInterceptor)(func: => T): T = {
    try {
      return func
    } catch {
      case e: AuthenticationException if maxAttempts > 1 => {
        // ignore error, refresh token and try again until limit is reached
        LOGGER.error("Auth token has expired.  Refreshing the token.")
        interceptor.token = getAuthToken()
        retryOnAuthTokenExpired((maxAttempts - 1), interceptor)(func)
      }
    }
  }

  private def getAuthToken(): String = {
    val backend = HttpURLConnectionBackend()

    //response.body -> Left(errorMessage), Right(body)
    val response = basicRequest
      .contentType(MediaType.ApplicationXWwwFormUrlencoded)
      .body(
          "grant_type" -> "client_credentials",
          "client_id" -> Configuration.keycloakAuthClientId,
          "client_secret" -> Configuration.keycloakAuthClientSecret
      )
      .post(uri"${Configuration.keycloakAuthUrl}").send(backend)

    backend.close

    if(StatusCode.Ok == response.code && response.body.toString.trim.length > 0){
      val jsonResponse: JsonObject = JsonParser.parseString(response.body.right.get).getAsJsonObject
      jsonResponse.get("access_token").getAsString
    }else{
      throw new RuntimeException(s"Failed to obtain access token from Keycloak.\n${response.body.left.get}")
    }
  }
}