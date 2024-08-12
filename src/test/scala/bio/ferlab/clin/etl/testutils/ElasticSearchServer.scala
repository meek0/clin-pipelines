package bio.ferlab.clin.etl.testutils

import bio.ferlab.clin.etl.conf.EsConf
import bio.ferlab.clin.etl.es.EsClient
import bio.ferlab.clin.etl.testutils.FhirTestUtils.getClass
import bio.ferlab.clin.etl.testutils.containers.{ElasticSearchContainer, FhirServerContainer}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.{HttpPost, HttpPut}
import org.apache.http.entity.StringEntity
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

trait ElasticSearchServer {
  val esLogger: Logger = LoggerFactory.getLogger(getClass)
  val (esPort, isEsNew) = ElasticSearchContainer.startIfNotRunning()
  val esBaseUrl = s"http://localhost:$esPort"
  val esConf = EsConf(esBaseUrl, null, null)
  val esClient = new EsClient(esConf)
}

trait ElasticSearchServerSuite extends ElasticSearchServer with TestSuite with BeforeAndAfterAll {
  val mapper = new ObjectMapper()

  def createEmptyIndex(index: String) = {
    val request = new HttpPut(s"${esBaseUrl}/$index")
    request.addHeader("Content-Type", "application/json")
    esLogger.info(s"Create empty index: $index")
    val (body, status) = esClient.executeHttpRequest(request)
    status match {
      case 200 => esLogger.info("Index created: " + body.getOrElse(""))
      case 400 => if (body.getOrElse("").contains("already exists")) esLogger.info("Index already exists: " + body.getOrElse(""))
                  else throw new IllegalStateException(s"Failed to create index: ${status}\n${body.getOrElse("")}")
      case _ => throw new IllegalStateException(s"Failed to create index: ${status}\n${body.getOrElse("")}")
    }
  }

  def createAndAddHPOs(index: String, testFile: String) = {
    createEmptyIndex(index)
    val request = new HttpPost(s"${esBaseUrl}/$index/_bulk")
    request.setEntity(new StringEntity(getHPOsResourceContent(testFile)))
    request.addHeader("Content-Type", "application/x-ndjson")
    esLogger.info(s"Add HPOs: $index")
    val (body, status) = esClient.executeHttpRequest(request)
    status match {
      case 200 => esLogger.info("HPOs added: " + body.getOrElse(""))
      case _ => throw new IllegalStateException(s"Failed to add HPOs: ${status}\n${body.getOrElse("")}")
    }
    Thread.sleep(2000) // ES has an internal delay for data to be available
  }

  private def getHPOsResourceContent(res: String) = {
    val resourceUrl = getClass.getResource(s"/hpos/$res")
    val source = Source.fromURL(resourceUrl)
    val content = source.mkString
    source.close()
    content
  }
}

object StartElasticSearchServer extends App with ElasticSearchServer {
  esLogger.info(s"ElasticSearch is started : $esPort")
  while (true) {

  }
}
