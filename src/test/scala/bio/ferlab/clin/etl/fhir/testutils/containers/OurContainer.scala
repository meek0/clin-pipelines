package bio.ferlab.clin.etl.fhir.testutils.containers

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import scala.collection.JavaConverters._
trait OurContainer {
  def container: GenericContainer

  private var isStarted = false

  def name: String

  def port: Option[Int]

  private var publicPort: Option[Int] = None

  def startIfNotRunning(): Option[Int] = {
    if (isStarted) {
      publicPort
    } else {
      val runningContainer = container.dockerClient.listContainersCmd().withLabelFilter(Map("name" -> name).asJava).exec().asScala

      runningContainer.toList match {
        case Nil =>
          container.start()
          publicPort = port.map { p => container.mappedPort(p) }
        case List(c) =>
          publicPort = port.flatMap { ourPort => c.ports.collectFirst { case p if p.getPrivatePort == ourPort => p.getPublicPort } }
      }
      isStarted = true
      publicPort
    }
  }


}
