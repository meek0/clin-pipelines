package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
import bio.ferlab.clin.etl.fhir.testutils.{FhirTestUtils, WithFhirServer}
import bio.ferlab.clin.etl.model._
import bio.ferlab.clin.etl.task.PatientValidation.validate
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class PatientValidationSpec extends FlatSpec with Matchers with GivenWhenThen with WithFhirServer {

  "validate" should "not return any error if patients match fhir resources" in {
    Given("A fhir server with some patients resources")
    val ptId1 = FhirTestUtils.loadPatients()
    val ptId2 = FhirTestUtils.loadPatients(firstName = "Jane", gender = FEMALE)

    And("A metadata object containing some patients that match fhir resources")
    val metadata = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId1)),
      defaultAnalysis.copy(patient = Patient(ptId2, "Jane", "Doe", "female"))
    ))


    When("Validate patients")
    val res = validate(metadata)(fhirServer.clinClient)

    Then("An empty list is returned")
    res shouldBe empty
  }

  it should "return errors if patients does not match fhir resources" in {
    Given("A fhir server with the same patients than metadata")
    val ptId1 = FhirTestUtils.loadPatients()


    And("A metadata object containing some patients")
    val metadata = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId1)),
      defaultAnalysis.copy(patient = defaultPatient("unknown_1")),
      defaultAnalysis.copy(patient = defaultPatient("unknown_2"))
    ))


    When("Validate patients")
    val res = validate(metadata)(fhirServer.clinClient)

    Then("A list containing missing resources")
    res shouldBe Seq("Patient unknown_1 does not exist", "Patient unknown_2 does not exist")
  }


  it should "return an error if patient first name does not match fhir resource" in {
    Given("A fhir server with patients")
    val ptId1 = FhirTestUtils.loadPatients()

    And("A metadata object containing the same patient than fhir resource, except for first name ")
    val metadata = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId1, firstName = "Robert"))
    ))

    When("Validate patients")
    val res = validate(metadata)(fhirServer.clinClient)

    Then("A list containing patient with first name mismatch is returned")
    res shouldBe Seq(s"Patient id=$ptId1 : First Name are not the same (Robert <-> John)")
  }

  it should "return an error if patients last name does not match fhir resource" in {
    Given("A fhir server with patients")
    val ptId1 = FhirTestUtils.loadPatients()

    And("A metadata object containing the same patient than fhir resource, except for last name ")
    val metadata = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId1, lastName = "River"))
    ))

    When("Validate patients")
    val res = validate(metadata)(fhirServer.clinClient)

    Then("A list containing patient with last name mismatch is returned")
    res shouldBe Seq(s"Patient id=$ptId1 : Last Name are not the same (River <-> Doe)")
  }

  it should "return an error if patients is inactive in fhir resource" in {
    Given("A fhir server with inactive patient")
    val ptId1 = FhirTestUtils.loadPatients(isActive = false)

    And("A metadata object containing patients thant match inactive patient in fhir")
    val metadata = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId1))
    ))


    When("Validate patients")
    val res = validate(metadata)(fhirServer.clinClient)

    Then("A list containing inactive patient is returned")
    res shouldBe Seq(s"Patient id=$ptId1 : patient is inactive")

  }

  it should "return an error if patients sex does not match with fhir resource" in {
    Given("A fhir server with patients")
    val ptId1 = FhirTestUtils.loadPatients()

    And("A metadata object containing the same patient than fhir resource, except for sex")
    val metadata = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId1, sex = "female"))
    ))

    When("Validate patients")
    val res = validate(metadata)(fhirServer.clinClient)

    Then("A list containing patient with sex mismatch is returned")
    res shouldBe Seq(s"Patient id=$ptId1 : Sex are not the same (female <-> male)")

  }

  //Like an integration test
  it should "return multiple errors if many entries dont match fhir resources" in {
    Given("A fhir server with patients")
    val ptId1 = FhirTestUtils.loadPatients()
    val ptId2 = FhirTestUtils.loadPatients()

    And("A metadata object containing a mix of mismatch patients")
    val metadata = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId1)),
      defaultAnalysis.copy(patient = defaultPatient("unknown")),
      defaultAnalysis.copy(patient = defaultPatient(ptId2, firstName="Jane", sex = "female"))
    ))

    When("Validate patients")
    val res = validate(metadata)(fhirServer.clinClient)

    Then("A list containing patient with sex mismatch is returned")
    res shouldBe Seq(
      "Patient unknown does not exist",
      s"Patient id=$ptId2 : First Name are not the same (Jane <-> John)",
      s"Patient id=$ptId2 : Sex are not the same (female <-> male)"
    )

  }

}
