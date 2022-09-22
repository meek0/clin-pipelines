package bio.ferlab.clin.etl.conf

import org.scalatest.{FlatSpec, Matchers}

class FerloadConfSpec extends FlatSpec with Matchers{
  "cleanedUrl" should "remove leading slash" in {
    FerloadConf("https://localhost:8080/").cleanedUrl shouldBe "https://localhost:8080"
  }

}
