package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.task.fileimport.model._
import bio.ferlab.clin.etl.task.fileimport.validation.SpecimenValidation.{validateSample, validateSpecimen}
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.{defaultAnalysis, defaultPatient}
import bio.ferlab.clin.etl.testutils.{FhirServerSuite, FhirTestUtils}
import cats.data.Validated.{Invalid, Valid}
import cats.data._
import org.scalatest.{FlatSpec, Matchers}

class SpecimenValidationSpec extends FlatSpec with Matchers with FhirServerSuite {

  "validateSpecimen" should "return a list of errors if specimen does not belong to the same patient" in {

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val ldmId = nextId()
    FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", ldmId)

    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmId, ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Specimen id=$ldmId : does not belong to the same patient (not_the_good_one_1 <-> $ptId1)"
      )
    )

  }

  it should "return a list of errors if specimen does not have the same type" in {

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val ldmId = nextId()
    FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", ldmId)
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmId, ldm = "CHUSJ", patient = defaultPatient(ptId1), specimenType = "TUMOR")
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Specimen id=$ldmId : does not have the same type (TUMOR <-> NBL)"
      )
    )

  }
  it should "return a list of errors if specimen type is invalid" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val ldmSpecimenId = nextId()
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmSpecimenId, ldm = "CHUSJ", patient = defaultPatient(ptId), specimenType = "invalid")
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        """Error Specimen : type - Unknown code {http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type}invalid for 'http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type#invalid'"""
      )
    )
  }

  it should "return a valid new specimen if it does not exist" in {
    val ldmSpecimenId = nextId()
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmSpecimenId, ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSpecimen(analysis) shouldBe Valid(TNewSpecimen("CHUSJ", ldmSpecimenId, "NBL", SpecimenType))
  }

  it should "return a valid existing specimen if it exist" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val ldmId = nextId()
    val spId = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", ldmId)
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmId, ldm = "CHUSJ", patient = defaultPatient(ptId))
    val validSpecimen: ValidationResult[TSpecimen] = validateSpecimen(analysis)

    validSpecimen match {
      case Valid(TExistingSpecimen(s)) =>
        s.getIdElement.getIdPart shouldBe spId
      case Valid(_) => fail("Expected an existing specimen")
      case Invalid(_) => fail("Expected a valid existing specimen")
    }
  }


  "validateSample" should "return a list of errors if sample does not belong to the same patient or does not have the same type" in {

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val ldmSpecimenId = nextId()
    val ldmSampleId = nextId()
    val sp1 = FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", ldmSpecimenId)
    val sa1 = FhirTestUtils.loadSpecimen(ptId1, "CHUSJ", ldmSampleId, parent = Some(sp1), level = "sample")

    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmSpecimenId, ldmSampleId = ldmSampleId, ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSample(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Sample id=$ldmSampleId : does not belong to the same patient (not_the_good_one_1 <-> $ptId1)",
        s"Sample id=$ldmSampleId : does not have the same type (DNA <-> NBL)"
      )
    )

  }

  it should "return a list of errors if sample type is invalid" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart

    val analysis = defaultAnalysis.copy(ldmSpecimenId = "1", ldm = "CHUSJ", patient = defaultPatient(ptId), specimenType = "NBL", sampleType = Some("invalid"))
    validateSample(analysis) shouldBe Invalid(
      NonEmptyList.of(
        """Error Sample : type - Unknown code {http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type}invalid for 'http://fhir.cqgc.ferlab.bio/CodeSystem/specimen-type#invalid'"""
      )
    )
  }

  it should "return a valid new sample if both specimen and sample does not exist" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val ldmSampleId = nextId()
    val ldmSpecimenId = nextId()
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmSpecimenId, ldmSampleId = ldmSampleId, ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSample(analysis) shouldBe Valid(TNewSpecimen("CHUSJ", ldmSampleId, "DNA", SampleType))
  }

  it should "return a valid new sample if sample does not exist but specimen does" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val ldmSpecimenId = nextId()
    val ldmSampleId = nextId()
    val sp1 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", ldmSpecimenId)
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmSpecimenId, ldmSampleId = ldmSampleId, ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSample(analysis) shouldBe Valid(TNewSpecimen("CHUSJ", ldmSampleId, "DNA", SampleType))
  }

  it should "return an error if samples does not have the same parent " in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val ldmSpecimenId1 = nextId()
    val ldmSpecimenId2 = nextId()
    val ldmSampleId = nextId()
    val sp1 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", ldmSpecimenId1)
    val sp2 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", ldmSpecimenId2)
    val sa1 = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", ldmSampleId, parent = Some(sp2), level = "sample", specimenType = "DNA")
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmSpecimenId1, ldmSampleId = ldmSampleId, ldm = "CHUSJ", patient = defaultPatient(ptId))
    validateSample(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Sample $ldmSampleId : parent specimen are not the same ($ldmSpecimenId1 <-> $ldmSpecimenId2 AND (https://cqgc.qc.ca/labs/CHUSJ/specimen <-> https://cqgc.qc.ca/labs/CHUSJ/specimen)"
      ))
  }

  it should "return a valid existing sample if it exist" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val ldmSpecimenId = nextId()
    val ldmSampleId = nextId()
    val spId = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", ldmSpecimenId)
    val saId = FhirTestUtils.loadSpecimen(ptId, "CHUSJ", ldmSampleId, parent = Some(spId), level = "sample", specimenType = "DNA")
    val analysis = defaultAnalysis.copy(ldmSpecimenId = ldmSpecimenId, ldmSampleId = ldmSampleId, ldm = "CHUSJ", patient = defaultPatient(ptId))
    val validSample: ValidationResult[TSpecimen] = validateSample(analysis)

    validSample match {
      case Valid(TExistingSpecimen(s)) =>
        s.getIdElement.getIdPart shouldBe saId
      case Valid(_) => fail("Expected an existing sample")
      case Invalid(_) => fail("Expected a valid existing sample")
    }
  }



}