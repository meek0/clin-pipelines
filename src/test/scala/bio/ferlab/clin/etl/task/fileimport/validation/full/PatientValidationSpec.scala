package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.task.fileimport.model.{TExistingPatient, TExistingPerson, TNewPatient, TNewPerson}
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.defaultFullPatient
import bio.ferlab.clin.etl.testutils.{FhirServerSuite, FhirTestUtils}
import cats.data.Validated.{Invalid, Valid}
import org.hl7.fhir.r4.model.codesystems.AdministrativeGender
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class PatientValidationSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {

  "validatePatient" should "return no errors if new patient" in {
    val ndm = nextId()
    When("Validate patient")
    val res = PatientValidation.validatePatient(defaultFullPatient(ramq = None, ndm = Some(ndm)))

    Then("A valid person is returned")
    res match {
      case Valid(p@TNewPatient(_)) =>
        p.resourcePatient.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.resourcePatient.getIdentifierFirstRep.getValue shouldBe ndm
        p.resourcePatient.getManagingOrganization.getReference shouldBe "Organization/CHUSJ"
      case Valid(TExistingPatient(_, _)) => fail("Expect a TNewPatient instance")
      case Invalid(_) => fail("Expect Valid")
    }

  }

  it should "return no errors if patient exist with the same ndm in the same ep" in {
    val ndm = nextId()
    Given("An existing organization")
    val orgIdEp1 = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")
    val orgIdEp2 = FhirTestUtils.loadOrganizations(id = "CHUM", name = "CHU Montreal", alias = "CHUM")

    And("A patient in metadata")
    val patientEp1 = defaultFullPatient(lastName = "Doe2", ndm = Some(ndm), ep = orgIdEp1)
    val patientEp2 = defaultFullPatient(ndm = Some(ndm), ep = orgIdEp2, sex = "female")

    And("And existing patient in fhir with same NDM and an organization")
    val ptIdEp1 = FhirTestUtils.loadPatients(managingOrganization = Some(orgIdEp1), identifier = Some(ndm))
    val ptIdEp2 = FhirTestUtils.loadPatients(managingOrganization = Some(orgIdEp2), identifier = Some(ndm))

    When("Validate patient EP1")
    val resEp1 = PatientValidation.validatePatient(patientEp1)

    Then("A valid patient is returned")
    resEp1 match {
      case Valid(TNewPatient(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPatient(_, _)) =>
        p.resourcePatient.getId shouldBe ptIdEp1.getValue
        p.resourcePatient.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.resourcePatient.getIdentifierFirstRep.getValue shouldBe ndm
        p.resourcePatient.getManagingOrganization.getReference shouldBe s"Organization/$orgIdEp1"

      case Invalid(_) => fail("Expect Valid")
    }

    When("Validate patient EP2")
    val resEp2 = PatientValidation.validatePatient(patientEp2)

    Then("A valid patient is returned")
    resEp2 match {
      case Valid(TNewPatient(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPatient(_, _)) =>
        p.resourcePatient.getId shouldBe ptIdEp2.getValue
        p.resourcePatient.getGender.toCode shouldBe AdministrativeGender.FEMALE.toCode
        p.resourcePatient.getIdentifierFirstRep.getValue shouldBe ndm
        p.resourcePatient.getManagingOrganization.getReference shouldBe s"Organization/$orgIdEp2"

      case Invalid(_) => fail("Expect Valid")
    }

  }

  it should "return no errors if patient exist with the same rqdm in the same ep" in {
    val ndm = nextId()
    val ramq = nextId()
    Given("An existing organization")
    val orgIdEp1 = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")
    val orgIdEp2 = FhirTestUtils.loadOrganizations(id = "CHUM", name = "CHU Montreal", alias = "CHUM")

    And("A patient in metadata")
    val patientEp1 = defaultFullPatient(lastName = "Doe2", ramq = Some(ramq), ep = orgIdEp1)
    val patientEp2 = defaultFullPatient(lastName = "Doe2", ramq = Some(ramq), ep = orgIdEp2)

    And("And existing patient in fhir with same NDM and an organization")
    val ptIdEp1 = FhirTestUtils.loadPatients(managingOrganization = Some(orgIdEp1), identifier = Some(ndm))
    val ptIdEp2 = FhirTestUtils.loadPatients(managingOrganization = Some(orgIdEp2), identifier = Some(ndm))

    And("An existing person in fhir associated to the patient")
    FhirTestUtils.loadPerson(ramq = Some(ramq), patientId = Seq(ptIdEp1.getIdPart, ptIdEp2.getIdPart))

    When("Validate patient EP1")
    val resEp1 = PatientValidation.validatePatient(patientEp1)

    Then("A valid patient is returned")
    resEp1 match {
      case Valid(TNewPatient(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPatient(_, _)) =>
        p.resourcePatient.getId shouldBe ptIdEp1.getValue
        p.resourcePatient.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.resourcePatient.getIdentifierFirstRep.getValue shouldBe ndm
        p.resourcePatient.getManagingOrganization.getReference shouldBe s"Organization/$orgIdEp1"

      case Invalid(_) => fail("Expect Valid")
    }
    When("Validate patient EP1")
    val resEp2 = PatientValidation.validatePatient(patientEp2)

    Then("A valid patient is returned")
    resEp2 match {
      case Valid(TNewPatient(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPatient(_, _)) =>
        p.resourcePatient.getId shouldBe ptIdEp2.getValue
        p.resourcePatient.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.resourcePatient.getIdentifierFirstRep.getValue shouldBe ndm
        p.resourcePatient.getManagingOrganization.getReference shouldBe s"Organization/$orgIdEp2"

      case Invalid(_) => fail("Expect Valid")
    }
  }

  it should "return no errors if patient exist with the same rqdm in the same ep with a ndm null in fhir resource" in {
    val ndm = nextId()
    val ramq = nextId()
    Given("An existing organization")
    val orgId = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")

    And("A patient in metadata")
    val patient = defaultFullPatient(lastName = "Doe2", ramq = Some(ramq), ndm = Some(ndm), ep = orgId)

    And("And existing patient in fhir with same NDM and an organization")
    val ptId = FhirTestUtils.loadPatients(managingOrganization = Some(orgId), identifier = None)

    And("An existing person in fhir associated to the patient")
    val psId = FhirTestUtils.loadPerson(ramq = Some(ramq), patientId = Seq(ptId.getIdPart))

    When("Validate patient")
    val res = PatientValidation.validatePatient(patient)

    Then("A valid patient is returned")
    res match {
      case Valid(TNewPatient(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPatient(_, _)) =>
        p.resourcePatient.getId shouldBe ptId.getValue
        p.resourcePatient.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.resourcePatient.getIdentifierFirstRep.getValue shouldBe ndm
        p.resourcePatient.getManagingOrganization.getReference shouldBe s"Organization/$orgId"

      case Invalid(_) => fail("Expect Valid")
    }


  }

  it should "return errors if patient exist with the same rqdm in the same ep with a ndm different in fhir resource" in {
    val ndmFullPatient = nextId()
    val ndmExistingPatient = nextId()
    val ramq = nextId()
    Given("An existing organization")
    val orgId = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")

    And("A patient in metadata")
    val patient = defaultFullPatient(lastName = "Doe2", ramq = Some(ramq), ndm = Some(ndmFullPatient), ep = orgId)

    And("And existing patient in fhir with same NDM and an organization")
    val ptId = FhirTestUtils.loadPatients(managingOrganization = Some(orgId), identifier = Some(ndmExistingPatient))

    And("An existing person in fhir associated to the patient")
    val psId = FhirTestUtils.loadPerson(ramq = Some(ramq), patientId = Seq(ptId.getIdPart))

    When("Validate patient")
    val res = PatientValidation.validatePatient(patient)

    Then("A valid patient is returned")
    res.isValid shouldBe false

  }
}
