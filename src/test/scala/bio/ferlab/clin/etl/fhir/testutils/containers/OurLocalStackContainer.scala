package bio.ferlab.clin.etl.fhir.testutils.containers

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName


case object OurLocalStackContainer extends OurContainer {
  val localStackContainer: LocalStackContainer = {
    val localstackImage = DockerImageName.parse("localstack/localstack:0.11.3")
    val localstack = new LocalStackContainer(localstackImage)
      .withServices(Service.S3)
    localstack
  }
  val container: GenericContainer = new GenericContainer(localStackContainer)

  override def name: String = "clin-pipelines-localstack"

  override val port: Option[Int] = None
}
