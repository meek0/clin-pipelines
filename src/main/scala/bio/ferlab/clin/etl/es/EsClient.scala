package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.conf.EsConf
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{HttpPost, HttpPut, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.util.Base64

class EsClient(conf: EsConf) {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  val client = HttpClientBuilder.create().build()
  val charset = StandardCharsets.UTF_8.name()

  def getHPOs(index: String, release: String): Seq[EsHPO] = {
    var allHPOs = Seq[EsHPO]()

    def getHPOWithPagination(from: String = "", size: Int = 1000): Unit = {
      val request = new HttpPost(s"${conf.url}/${index}_${release}/_search")
      request.setEntity(new StringEntity(
        """
            {
                "size": {size},
                "_source": ["hpo_id", "name"],
                "search_after": [ "{from}" ],
                "sort": [
                    {"_id": "asc"}
                ]
            }
            """.replace("{size}", String.valueOf(size)).replace("{from}", from)
      ))
      request.addHeader("Content-Type", "application/json")
      LOGGER.info(s"Fetch HPOs from ES from: ${allHPOs.size} size: $size")
      val (body, status) = executeHttpRequest(request)
      status match {
        case 200 => {
          val (lastId, currentHPOs) = parseHPOs(body.getOrElse(""))
          if (StringUtils.isNotBlank(lastId)) {
            allHPOs ++= currentHPOs
            getHPOWithPagination(lastId, size)
          }
        }
        case _ => throw new IllegalStateException(s"Failed to get HPOs from ES: ${status}\n${body.getOrElse("")}")
      }
    }

    getHPOWithPagination()
    allHPOs.groupBy(_.hpo_id).values.map(_.head).toSeq.sortBy(_.hpo_id)
  }

  def publishHPO(index: String, release: String, alias: String): Unit = {
    val request = new HttpPost(s"${conf.url}/_aliases")
    request.setEntity(new StringEntity(
    """
      {
          "actions": [
              { "remove": { "index": "*", "alias": "{alias}" } },
              { "add": { "index": "{index}_{release}", "alias": "{alias}" } }
          ]
      }
    """.replace("{alias}", alias).replace("{index}", index).replace("{release}", release)
    ))
    request.addHeader("Content-Type", "application/json")
    LOGGER.info(s"Publish HPOs release: ${index}_${release} => $alias")
    val (body, status) = executeHttpRequest(request)
    status match {
      case 200 => LOGGER.info("HPOs release published: " + body.getOrElse(""))
      case _ => throw new IllegalStateException(s"Failed to publish HPOs release: ${status}\n${body.getOrElse("")}")
    }
  }

  private def parseHPOs(body: String): (String, Seq[EsHPO]) = {
    var hpos = Seq[EsHPO]()
    var lastId = ""
    if (StringUtils.isNotBlank(body)) {
      val json = new JSONObject(body)
      val hits = json.getJSONObject("hits").getJSONArray("hits")
      hits.forEach(hit => {
        lastId = hit.asInstanceOf[JSONObject].getString("_id")
        val source = hit.asInstanceOf[JSONObject].getJSONObject("_source")
        val hpo = EsHPO(source.getString("hpo_id"), source.getString("name"))
        hpos :+= hpo
      })
    }
    (lastId, hpos)
  }

  def executeHttpRequest(request: HttpRequestBase): (Option[String], Int) = {
    addBasicAuthHeader(request)
    val response: HttpResponse = client.execute(request)
    val body = Option(response.getEntity).map(e => EntityUtils.toString(e, charset))
    // always properly close
    EntityUtils.consumeQuietly(response.getEntity)
    (body, response.getStatusLine.getStatusCode)
  }

  private def addBasicAuthHeader(request: HttpRequestBase): Unit = {
    if (StringUtils.isNoneBlank(conf.user, conf.password)) {
      val auth = Base64.getEncoder.encodeToString(s"${conf.user}:${conf.password}".getBytes(charset))
      request.addHeader("Authorization", s"Basic $auth")
    }
  }

}
