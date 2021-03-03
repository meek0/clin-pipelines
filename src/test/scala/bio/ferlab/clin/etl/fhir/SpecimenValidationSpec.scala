package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils.{defaultAnalysis, defaultPatient}
import bio.ferlab.clin.etl.fhir.testutils.{FhirServerSuite, FhirTestUtils}
import bio.ferlab.clin.etl.task.validation.SpecimenValidation.{validateSample, validateSpecimen}
import cats.data.Validated.Invalid
import cats.data._
import org.scalatest.{FlatSpec, Matchers}

class SpecimenValidationSpec extends FlatSpec with Matchers with FhirServerSuite {

  "validate samples" should "return a list of errors if samples are not associated to anothers donors" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val ptId2 = FhirTestUtils.loadPatients().getIdPart
    val spId1 = FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", "1")
    val spId3 = FhirTestUtils.loadSpecimen(ptId1, "CHUM", "1")
    val spId2 = FhirTestUtils.loadSpecimen(ptId2, "CHUSJ", "2")
    val metadata = defaultMetadata.copy(
      analyses = Seq(
        defaultAnalysis.copy(specimenId = "1", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1")),
        defaultAnalysis.copy(specimenId = "2", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_2"))
      )
    )
    val analysis = defaultAnalysis.copy(specimenId = "1", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Specimen id=1 : does not belong to the same patient (not_the_good_one_1 <-> $ptId1)"
      )
    )


  }

  it should "return a corresponding map of sapmple ids if samples are associated to right donors" in {


  }

  "test" should "deede" in {
    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val parent = FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", "1")
    val sample = FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", "2", parent = Some(parent))
    val analysis = defaultAnalysis.copy(specimenId = "1", sampleId="2", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSample(analysis)
  }
}
