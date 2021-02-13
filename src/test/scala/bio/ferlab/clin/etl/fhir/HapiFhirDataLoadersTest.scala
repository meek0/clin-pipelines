package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.{FhirTestUtils, WithFhirServer}
import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, ForAllTestContainer}
import org.hl7.fhir.r4.model.{DocumentReference, Specimen}
import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

class HapiFhirDataLoadersTest extends FreeSpec with Matchers with WithFhirServer {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

//
//  "Specimen and samples" - {
//    "Specimen" in {
//      val organisationId = FhirTestUtils.loadOrganizations()
//      val patientId = FhirTestUtils.loadPatients("Bambois", "Jean")
//      val specimens = FhirTestUtils.loadSpecimens(patientId, organisationId)
//      specimens.isEmpty shouldBe false
//
//      // Search Hapi Fhir for a specimen we just created
//      val specimen: Option[Specimen] = FhirTestUtils.findById(specimens.head.getId, classOf[Specimen])
//
//      specimen.isDefined shouldBe true
//      specimen.get.getId should not be empty
//    }
//    "Samples" in {
//      val organisationId = FhirTestUtils.loadOrganizations()
//      val patientId = FhirTestUtils.loadPatients("Bambois", "Jean")
//      val specimens = FhirTestUtils.loadSpecimens(patientId, organisationId)
//      val samples: Seq[Specimen] = FhirTestUtils.loadSamples(patientId, organisationId, specimens.map(s => s.getId -> s)(collection.breakOut))
//      samples.isEmpty shouldBe false
//
//      // Search Hapi Fhir for a sample we just created
//      val sample: Option[Specimen] = FhirTestUtils.findById(samples.head.getId, classOf[Specimen])
//
//      sample.isDefined shouldBe true
//      sample.get.getId should not be empty
//    }
//  }

  /*
  "Files" - {
    "Linked CRAM and CRAI" in {
      val organisationId = FhirTestUtils.loadOrganizations()
      val patientId = FhirTestUtils.loadPatients("Bambois", "Jean")
      val specimens = FhirTestUtils.loadSpecimens(patientId, organisationId)
      val files = FhirTestUtils.loadFiles(patientId, organisationId,specimens)
      // files.foreach(f => FhirTestUtils.printJson(f))

      val file: Option[DocumentReference] = FhirTestUtils.findById(files.head.getId, classOf[DocumentReference])
      file.isDefined shouldBe true

      file.get.getId should not be empty
    }
  }

    "Analysis" - {
      "Seuqencing Alignment" in {
        val analyses: Seq[DocumentManifest] = FhirTestUtils.loadAnalyses(files)

        val analysis: Option[DocumentManifest] = FhirTestUtils.findById(analyses.head.getId, classOf[DocumentManifest])
        analysis.isDefined shouldBe true

        analysis.get.getId should not be empty
      }
    }
     */
}
