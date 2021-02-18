package bio.ferlab.clin.etl.fhir.testutils.containers

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import scala.collection.JavaConverters._
case object FhirServerContainer extends OurContainer {
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
  val name = "clin-pipeline-fhir-test"
  private val httpPort = 8080
  val port = Some(httpPort)
  val container = GenericContainer(
    "chusj/clin-fhir-server:latest",
    waitStrategy = Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(60)),
    exposedPorts = Seq(httpPort),
    env = fhirEnv,
    labels = Map("name" -> name)
  )
}
