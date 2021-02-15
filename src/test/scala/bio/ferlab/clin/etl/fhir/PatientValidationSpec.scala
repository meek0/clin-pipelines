package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
import bio.ferlab.clin.etl.fhir.testutils.{FhirTestUtils, WithFhirServer}
import bio.ferlab.clin.etl.task.validation.PatientValidation.validatePatient
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import org.hl7.fhir.r4.model.IdType
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class PatientValidationSpec extends FlatSpec with Matchers with GivenWhenThen with WithFhirServer {

  "validate" should "not return any error if patient match fhir resource" in {
    Given("A fhir server with a patient resource")
    val ptId1 = FhirTestUtils.loadPatients()

    And("A patient that match fhir resource")
    val patient = defaultPatient(ptId1.getIdPart)

    When("Validate patient")
    val res = validatePatient(patient)

    Then("A valid patient is returned")
    res shouldBe Valid(ptId1)
  }

  it should "return errors if patients does not match fhir resource" in {
    Given("A fhir server with a patient resource")
    FhirTestUtils.loadPatients()

    And("A patient that does not match fhir resource")
    val patient = defaultPatient("unknown_1")

    When("Validate patient")

    val res = validatePatient(patient)
    Then("An error is returned")
    res shouldBe Invalid(NonEmptyList.one("Patient unknown_1 does not exist"))
  }


  it should "return an error if patient first name does not match fhir resource" in {
    Given("A fhir server with a patient resource")
    val ptIdPart = FhirTestUtils.loadPatients().getIdPart

    And("A metadata object containing the same patient than fhir resource, except for first name ")
    val patient = defaultPatient(ptIdPart, firstName = "Robert")

    When("Validate patient")
    val res = validatePatient(patient)

    Then("An error is returned")
    res shouldBe Invalid(NonEmptyList.one(s"Patient id=$ptIdPart : First Name are not the same (Robert <-> John)"))

  }

  it should "return an error if patients last name does not match fhir resource" in {
    Given("A fhir server with a patient resource")
    val ptIdPart = FhirTestUtils.loadPatients().getIdPart

    And("A metadata object containing the same patient than fhir resource, except for last name ")
    val patient = defaultPatient(ptIdPart, lastName = "River")

    When("Validate patient")
    val res = validatePatient(patient)

    Then("An error is returned")
    res shouldBe Invalid(NonEmptyList.one(s"Patient id=$ptIdPart : Last Name are not the same (River <-> Doe)"))
  }

  it should "return an error if patients is inactive in fhir resource" in {
    Given("A fhir server with inactive patient")
    val ptIdPart = FhirTestUtils.loadPatients(isActive = false).getIdPart

    And("A metadata object containing patients thant match inactive patient in fhir")
    val patient = defaultPatient(ptIdPart)

    When("Validate patients")
    val res = validatePatient(patient)

    Then("An error is returned")
    res shouldBe Invalid(NonEmptyList.one((s"Patient id=$ptIdPart : patient is inactive")))

  }

    it should "return an error if patients sex does not match with fhir resource" in {
      Given("A fhir server with patient")
      val ptIdPart = FhirTestUtils.loadPatients().getIdPart

      And("A metadata object containing the same patient than fhir resource, except for sex")
      val patient = defaultPatient(ptIdPart, sex = "female")

      When("Validate patient")
      val res = validatePatient(patient)

      Then("A list containing patient with sex mismatch is returned")
      res shouldBe Invalid(NonEmptyList.one((s"Patient id=$ptIdPart : Sex are not the same (female <-> male)")))

    }

    //Like an integration test
    it should "return multiple errors if many entries dont match fhir resources" in {
      Given("A fhir server with patient")
      val ptIdPart = FhirTestUtils.loadPatients().getIdPart

      And("A patient that does not match fhir resources attributes")
      val patient = defaultPatient(ptIdPart, firstName="Jane", lastName="River", sex = "female")

      When("Validate patient")
      val res = validatePatient(patient)

      Then("A list of many errors is returned")
      res shouldBe Invalid(NonEmptyList.of(
        s"Patient id=$ptIdPart : First Name are not the same (Jane <-> John)",
        s"Patient id=$ptIdPart : Last Name are not the same (River <-> Doe)",
        s"Patient id=$ptIdPart : Sex are not the same (female <-> male)"
      ))

    }

}
