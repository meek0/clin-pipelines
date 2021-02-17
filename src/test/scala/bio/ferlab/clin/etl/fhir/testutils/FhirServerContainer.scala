package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import org.hl7.fhir.r4.model.IdType
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

object FhirServerContainer {
  val exposedPort: Int = 18080
  val fhirEnv: Map[String, String] = Map(
    "HAPI_DATASOURCE_URL" -> "jdbc:h2:mem:hapi",
    "HAPI_DATASOURCE_DRIVER" -> "org.h2.Driver",
    "HAPI_DATASOURCE_USERNAME" -> "",
    "HAPI_DATASOURCE_PASSWORD" -> "",
    "HAPI_AUTH_ENABLED" -> "false",
    "HAPI_AUDITS_ENABLED" -> "false",
    "HAPI_BIO_ELASTICSEARCH_ENABLED" -> "false",
    "HAPI_LOGGING_INTERCEPTOR_SERVER_ENABLED" -> "false",
    "HAPI_LOGGING_INTERCEPTOR_CLIENT_ENABLED" -> "false",
    "HAPI_SERVER_ADDRESS" -> s"http://localhost:$exposedPort/fhir/",
    "HAPI_VALIDATE_RESPONSES_ENABLED" -> "false",
    "JAVA_OPTS" -> "-Dhibernate.dialect=org.hibernate.dialect.H2Dialect"
  )
  private var isStarted = false

  lazy val container = FixedHostPortGenericContainer(

    "chusj/clin-fhir-server:latest",
    waitStrategy = Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(60)),
    exposedHostPort = exposedPort,
    exposedContainerPort = 8080,
    env = fhirEnv,
  )

  def start() = {
    if (System.getenv("startFhirServer") == "true") {
      if (!isStarted) {
        container.start()
        isStarted = true

      }
    } else {
      isStarted = true
    }
    exposedPort
  }

  def forceStart() = {
    if (!isStarted) {
      container.start()
      isStarted = true
    }

  }

}

trait WithFhirServer extends TestSuite with BeforeAndAfterAll {

  val port = FhirServerContainer.start()
  implicit val fhirServer = new FhirServerContainer(s"http://localhost:${port}/fhir")
  implicit val clinFhirClient = fhirServer.clinClient
  implicit val fhirClient = fhirServer.fhirClient

  override def afterAll() = {
    FhirTestUtils.clearAll()
    println("Shutting down.")
  }
}

class FhirServerContainer(val fhirBaseUrl: String) {
  val fhirContext: FhirContext = FhirContext.forR4()
  fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
  fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)

  val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], fhirBaseUrl)
  val fhirClient: IGenericClient = fhirContext.newRestfulGenericClient(fhirBaseUrl)
  val parser: IParser = fhirContext.newJsonParser().setPrettyPrint(true)
}


object AppFhirServer extends App {
  while (true) {
    FhirServerContainer.forceStart()
  }
}

object test extends App with WithFhirServer {
  val ptId = FhirTestUtils.loadPatients().getIdPart
  FhirTestUtils.loadOrganizations()
  FhirTestUtils.loadServiceRequest(patientId = ptId)
//  fhirClient.delete().resourceById(new IdType("Specimen", "5")).execute()
//  FhirTestUtils.clearAll()

}

object delete extends App with WithFhirServer {
    fhirClient.delete().resourceById(new IdType("Specimen", "15")).execute()
    fhirClient.delete().resourceById(new IdType("Specimen", "14")).execute()
  //  FhirTestUtils.clearAll()

}

