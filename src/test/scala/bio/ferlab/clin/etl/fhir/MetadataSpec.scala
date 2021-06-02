package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.task.fileimport.model.Metadata
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json._

class MetadataSpec extends FlatSpec with Matchers {

  "parse" should "parse json file" in {

    val metadata = Json.parse(getClass.getResourceAsStream("/metadata.json")).as[Metadata]

    println(metadata)
  }

}
