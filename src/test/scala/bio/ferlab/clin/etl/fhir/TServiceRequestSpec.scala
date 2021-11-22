package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.task.fileimport.model.TServiceRequest
import org.hl7.fhir.r4.model.{Reference, ServiceRequest}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class TServiceRequestSpec extends FlatSpec with Matchers {

  "buildResource" should "return a service request without any duplicate specimen" in {
    val sr = new ServiceRequest()
    sr.addSpecimen(new Reference("Specimen/1"))
    sr.addSpecimen(new Reference("Specimen/3"))
    val tsr = TServiceRequest(sr)

    val r = tsr.buildResource(new Reference("Specimen/1"), new Reference("Specimen/2"))
    r shouldBe defined
    r.get.getSpecimen.asScala.map(_.getReference) should contain theSameElementsAs Seq("Specimen/1", "Specimen/2", "Specimen/3")


  }

  "buildResource" should "return None if there is specimen, sample and aliquot already associated to the service reequest" in {
    val sr = new ServiceRequest()
    sr.addSpecimen(new Reference("Specimen/1"))
    sr.addSpecimen(new Reference("Specimen/2"))
    val tsr = TServiceRequest(sr)

    val r = tsr.buildResource(new Reference("Specimen/1"), new Reference("Specimen/2"))
    r shouldBe None

  }

}
