package bio.ferlab.clin.etl.task

import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import ca.uhn.fhir.rest.server.exceptions.AuthenticationException
import bio.ferlab.clin.etl.fhir._
import bio.ferlab.clin.etl.interceptor.AuthTokenInterceptor
import org.hl7.fhir.r4.model.{Bundle, ServiceRequest, Specimen}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json
import sttp.model.{MediaType, StatusCode}
import sttp.client3._

object LoadHapiFhirDataTask {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  val authRequestMaxRetries: Int = 3

  val fhirContext: FhirContext = FhirContext.forR4()
  fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
  fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)

  val jsonParser: IParser = fhirContext.newJsonParser().setPrettyPrint(true)

  def run(hapiFhirServerBase: String): Unit = {
    // The creation of the client is an expensive operation so, make sure not to create
    // a new client for each request.

    val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], hapiFhirServerBase)
    val client: IGenericClient = fhirContext.newRestfulGenericClient(hapiFhirServerBase)
    val authToken: String = getAuthToken()

    val hapiFhirInterceptor: AuthTokenInterceptor = new AuthTokenInterceptor(authToken)
    clinClient.registerInterceptor(hapiFhirInterceptor)
    client.registerInterceptor(hapiFhirInterceptor)

    val specimens: Seq[Specimen] = SpecimenLoader.loadSpecimens(clinClient)
    val samples: Seq[Specimen] = SpecimenLoader.loadSamples(clinClient, specimens.map(a => a.getId -> a).toMap)

    //vferretti decided to put these on hold for now.
    //val nanuqServiceRequests: Seq[ServiceRequest] = SequencingServiceRequestLoader.load(clinClient, specimens)



    createBundle(client, hapiFhirInterceptor, (specimens ++ samples))

  }

  private def createBundle(client: IGenericClient, interceptor: AuthTokenInterceptor, specimensAndSamples: Seq[Specimen]): Unit = {
    try {
      val bundle: Bundle = new Bundle()
      bundle.setType(Bundle.BundleType.TRANSACTION)

      specimensAndSamples.foreach(specimen => {
        bundle.addEntry()
          .setFullUrl(specimen.getIdElement.getValue)
          .setResource(specimen)
          .getRequest
          .setUrl("Specimen")
          //.setIfNoneExist - for conditional creation
          .setMethod(Bundle.HTTPVerb.POST)
      })

      retryOnAuthTokenExpired(authRequestMaxRetries, interceptor) {
        val response: Bundle = client.transaction().withBundle(bundle).execute()
        LOGGER.debug(jsonParser.encodeResourceToString(response))
      }
    } catch {
      case e: Throwable => {
        // TODO: Determine how to handle a failure to save a specimen or batch of specimens in Fhir
        // TODO: Log the error and stop or continue processing the rest of the specimens?  Rollback?
        LOGGER.error(s"An error occurred tyring to save specimen in HAPI Fhir.", e)
        throw e
      }
    }
  }

  def retryOnAuthTokenExpired[T](maxAttempts: Int, interceptor: AuthTokenInterceptor)(func: => T): T = {
    try {
      return func
    } catch {
      case e: AuthenticationException if maxAttempts > 1 => {
        // ignore error, refresh token and try again until limit is reached
        LOGGER.warn("Auth token has expired.  Refreshing the token.")
        interceptor.token = getAuthToken()
        retryOnAuthTokenExpired((maxAttempts - 1), interceptor)(func)
      }
    }
  }

  private def getAuthToken(): String = {
    val backend = HttpURLConnectionBackend()

    //response.body : Left(errorMessage), Right(body)
    val response = basicRequest
      .contentType(MediaType.ApplicationXWwwFormUrlencoded)
      .body(
        "grant_type" -> "client_credentials",
        "client_id" -> Configuration.keycloakAuthClientId,
        "client_secret" -> Configuration.keycloakAuthClientSecret
      )
      .post(uri"${Configuration.keycloakAuthUrl}").send(backend)

    backend.close

    if (StatusCode.Ok == response.code && response.body.toString.trim.length > 0) {
       (Json.parse(response.body.right.get) \ "access_token").as[String]

//     Ø val jsonResponse: JsonObject = JsonParser.parseString(response.body.right.get).getAsJsonObject
//      jsonResponse.get("access_token").getAsString
    } else {
      throw new RuntimeException(s"Failed to obtain access token from Keycloak.\n${response.body.left.get}")
    }
  }
}
