package bio.ferlab.clin.etl.fhir

import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}

object FhirClient {
  def buildFhirClients() = {
    val fhirServerUrl = sys.env("FHIR_SERVER_URL")//http://localhost:49160/fhir
    val fhirContext: FhirContext = FhirContext.forR4()
    fhirContext.getRestfulClientFactory.setConnectTimeout(120 * 1000)
    fhirContext.getRestfulClientFactory.setSocketTimeout(120 * 1000)
    fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
    fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)

    val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], fhirServerUrl)
    val client: IGenericClient = fhirContext.newRestfulGenericClient(fhirServerUrl)
    val hapiFhirInterceptor: AuthTokenInterceptor = new AuthTokenInterceptor()
    clinClient.registerInterceptor(hapiFhirInterceptor)
    client.registerInterceptor(hapiFhirInterceptor)

    (clinClient, client)
  }
}
