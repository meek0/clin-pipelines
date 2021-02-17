package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, GenericContainer}
import org.hl7.fhir.r4.model.IdType
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import org.testcontainers.containers.wait.strategy.Wait
import scala.collection.JavaConverters._
import java.time.Duration

object FhirServerContainer {
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
    "HAPI_VALIDATE_RESPONSES_ENABLED" -> "false",
    "JAVA_OPTS" -> "-Dhibernate.dialect=org.hibernate.dialect.H2Dialect"
  )
  private var isStarted = false

  private val CONTAINER_NAME = "clin-pipeline-fhir-test"
  lazy val container = GenericContainer(
    "chusj/clin-fhir-server:latest",
    waitStrategy = Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(60)),
    exposedPorts = Seq(8080),
    env = fhirEnv,
    labels = Map("name" -> CONTAINER_NAME)
  )

  def start(): Int = {

    val runningContainer = container.dockerClient.listContainersCmd().withLabelFilter(Map("name" -> CONTAINER_NAME).asJava).exec().asScala

    runningContainer.toList match {
      case Nil =>
        container.start()
        val port = container.mappedPort(8080)
        println(s"Container start, port=${port}")
        port
      case List(c) =>
        c.ports.collectFirst { case p if p.getPrivatePort == 8080 => p.getPublicPort }.get

    }

  }

}

trait WithFhirServer extends TestSuite with BeforeAndAfterAll {

  val port: Int = FhirServerContainer.start()
  implicit val fhirServer: FhirServerContainer = new FhirServerContainer(s"http://localhost:${port}/fhir")
  implicit val clinFhirClient: IClinFhirClient = fhirServer.clinClient
  implicit val fhirClient: IGenericClient = fhirServer.fhirClient

  override def afterAll(): Unit = {
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
    FhirServerContainer.start()
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

