package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.{Conf, EsConf, FhirConf, KeycloakConf}
import bio.ferlab.clin.etl.testutils.WholeStackSuite
import junit.framework.TestCase
import org.scalatest.{FlatSpec, Matchers}

class PublishHpoTermsTest  extends FlatSpec with WholeStackSuite with Matchers {

  val keycloakConf = KeycloakConf(null, null, null, null, null)
  val fhirConf = FhirConf(fhirBaseUrl)
  val conf = new Conf(awsConf, keycloakConf, fhirConf, ferloadConf, null, null, esConf)

  "PublishHpoTerms" should "failed if missing params" in {
      try {
        PublishHpoTerms("", "", "", Seq())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.equals(s"Expecting params: <index> <release> <alias>"))
      }
  }

  "PublishHpoTerms" should "failed if invalid index" in {
    try {
      PublishHpoTerms("not_found_index", "re_000", "hpo", Seq())(conf)
      fail("Expecting IllegalStateException")
    } catch {
      case e: IllegalStateException =>
        println(e.getMessage)
        assert(e.getMessage.startsWith(s"Failed to get HPOs from ES: 404"))
    }
  }

  "PublishHpoTerms" should "failed if empty index" in {
    try {
      createEmptyIndex("empty_index_re_000")
      PublishHpoTerms("empty_index", "re_000", "hpo", Seq())(conf)
      fail("Expecting IllegalStateException")
    } catch {
      case e: IllegalStateException =>
        println(e.getMessage)
        assert(e.getMessage.startsWith(s"Failed to get HPOs from ES: 400"))
    }
  }

  "PublishHpoTerms" should "publish FHIR and roll alias ES" in {
    createAndAddHPOs("index_1_re_000", "hpos_test_1.json")
    PublishHpoTerms("index_1", "re_000", "hpo", Seq())(conf)
  }

}