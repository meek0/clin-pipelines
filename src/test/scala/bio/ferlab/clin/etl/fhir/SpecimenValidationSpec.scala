package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.testutils.{FhirServerSuite, FhirTestUtils}
import bio.ferlab.clin.etl.task.fileimport.model._
import bio.ferlab.clin.etl.task.fileimport.validation.SpecimenValidation.{validateSample, validateSpecimen}
import cats.data.Validated.{Invalid, Valid}
import cats.data._
import org.scalatest.{FlatSpec, Matchers}

class SpecimenValidationSpec extends FlatSpec with Matchers with FhirServerSuite {

  "validateSpecimen" should "return a list of errors if specimen does not belong to the same patient" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", "1")

    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Specimen id=1 : does not belong to the same patient (not_the_good_one_1 <-> $ptId1)"
      )
    )

  }

  it should "return a list of errors if specimen does not have the same type" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", "1")

    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId1), specimenType = "BRSH")
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Specimen id=1 : does not have the same type (BRSH <-> NBL)"
      )
    )

  }
  it should "return a list of errors if specimen type is invalid" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId), specimenType = "invalid")
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        """Error Specimen : type - Unknown code {http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type}invalid for "http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type#invalid""""
      )
    )
  }

  it should "return a valid new specimen if it does not exist" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSpecimen(analysis) shouldBe Valid(TNewSpecimen("CHUSJ", "1", "NBL", "2053", SpecimenType))
  }

  it should "return a valid existing specimen if it exist" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val spId = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", "1")
    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId))
    val validSpecimen: ValidationResult[TSpecimen] = validateSpecimen(analysis)

    validSpecimen match {
      case Valid(TExistingSpecimen(s)) =>
        s.getIdElement.getIdPart shouldBe spId
      case Valid(_) => fail("Expected an existing specimen")
      case Invalid(_) => fail("Expected a valid existing specimen")
    }
  }


  "validateSample" should "return a list of errors if sample does not belong to the same patient or does not have the same type" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val sp1 = FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", "1")
    val sa1 = FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", "1", parent = Some(sp1), level = "sample")

    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldmSampleId = "1", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSample(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Sample id=1 : does not belong to the same patient (not_the_good_one_1 <-> $ptId1)",
        "Sample id=1 : does not have the same type (DNA <-> NBL)"
      )
    )

  }

  it should "return a list of errors if sample type is invalid" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart

    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId), specimenType = "NBL", sampleType = Some("invalid"))
    validateSample(analysis) shouldBe Invalid(
      NonEmptyList.of(
        """Error Sample : type - Unknown code {http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type}invalid for "http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type#invalid""""
      )
    )
  }

  it should "return a valid new sample if both specimen and sample does not exist" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldmSampleId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSample(analysis) shouldBe Valid(TNewSpecimen("CHUSJ", "1", "DNA", "2053", SampleType))
  }

  it should "return a valid new sample if sample does not exist but specimen does" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val sp1 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", "1")
    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldmSampleId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSample(analysis) shouldBe Valid(TNewSpecimen("CHUSJ", "1", "DNA", "2053", SampleType))
  }
  it should "return an error if samples does not have the same parent " in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val sp1 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", "1")
    val sp2 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", "2")
    val sa1 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", "1", parent = Some(sp2), level = "sample", specimenType = "DNA")
    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldmSampleId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSample(analysis) shouldBe Invalid(
      NonEmptyList.of(
        "Sample 1 : parent specimen are not the same (1 <-> 2 AND (https://cqgc.qc.ca/labs/CHUSJ/specimen <-> https://cqgc.qc.ca/labs/CHUSJ/specimen)"
      ))
  }

  it should "return a valid existing sample if it exist" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val spId = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", "1")
    val saId = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", "1", parent = Some(spId), level = "sample", specimenType = "DNA")
    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldmSampleId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId))
    val validSample: ValidationResult[TSpecimen] = validateSample(analysis)

    validSample match {
      case Valid(TExistingSpecimen(s)) =>
        s.getIdElement.getIdPart shouldBe saId
      case Valid(_) => fail("Expected an existing sample")
      case Invalid(_) => fail("Expected a valid existing sample")
    }
  }



}
