package bio.ferlab.clin.etl.testutils.containers

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

case object ElasticSearchContainer extends OurContainer {
  val name = "clin-pipeline-es-test"
  val port = 9200
  val container: GenericContainer = GenericContainer(
    "docker.elastic.co/elasticsearch/elasticsearch:7.8.1",
    exposedPorts = Seq(port),
    waitStrategy = Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(120)),
    labels = Map("name" -> name),
    env = Map("xpack.security.enabled" -> "false", "discovery.type" -> "single-node")
  )
}
