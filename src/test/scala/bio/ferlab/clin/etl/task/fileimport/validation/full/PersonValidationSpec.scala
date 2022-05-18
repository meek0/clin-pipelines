package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.task.fileimport.model.{TExistingPerson, TNewPerson}
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.defaultFullPatient
import bio.ferlab.clin.etl.testutils.{FhirServerSuite, FhirTestUtils}
import cats.data.Validated.{Invalid, Valid}
import org.hl7.fhir.r4.model.codesystems.AdministrativeGender
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class PersonValidationSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {

  "validatePerson" should "return no errors if new person" in {
    val ramq = nextId()
    When("Validate person")
    val res = PersonValidation.validatePerson(defaultFullPatient(ramq = Some(ramq)))

    Then("A valid person is returned")
    res match {
      case Valid(p@TNewPerson(_)) =>
        p.person.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.person.getNameFirstRep.getFamily shouldBe "Doe"
        p.person.getNameFirstRep.getGivenAsSingleString shouldBe "John"
        p.person.getIdentifierFirstRep.getValue shouldBe ramq
      case Valid(TExistingPerson(_, _)) => fail("Expect a TNewPerson instance")
      case Invalid(_) => fail("Expect Valid")
    }

  }

  it should "return no errors if person exist with the same ramq" in {
    val ramq = nextId()
    Given("A patient in metadata")
    val patient = defaultFullPatient(lastName = "Doe2", ramq = Some(ramq))

    And("An existing person with the same ramq in fhir")
    val psId = FhirTestUtils.loadPerson(ramq = patient.ramq)

    When("Validate person")
    val res = PersonValidation.validatePerson(patient)

    Then("A valid person is returned")
    res match {
      case Valid(TNewPerson(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPerson(_, _)) =>
        p.person.getId shouldBe psId.getValue
        p.person.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.person.getNameFirstRep.getFamily shouldBe "Doe2"
        p.person.getNameFirstRep.getGivenAsSingleString shouldBe "John"
        p.person.getIdentifierFirstRep.getValue shouldBe ramq

      case Invalid(_) => fail("Expect Valid")
    }

  }

  it should "return no errors if person exist for a patient with the same ndm and ep" in {
    val ramq = nextId()
    val ndm = nextId()
    Given("An existing organization")
    val orgId = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")

    And("And existing patient in fhir with a NDM and an organization")
    val ptId = FhirTestUtils.loadPatients(managingOrganization = Some(orgId), identifier = Some(ndm))

    And("An existing person in fhir associated to the patient")
    val psId = FhirTestUtils.loadPerson(ramq = Some(ramq), patientId = Seq(ptId.getIdPart))
    And("A patient in metadata")
    val patient = defaultFullPatient(lastName = "Doe2", ramq = None, ndm = Some(ndm), ep = orgId)
    When("Validate person")
    val res = PersonValidation.validatePerson(patient)

    Then("A valid person is returned")
    res match {
      case Valid(TNewPerson(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPerson(_, _)) =>
        p.person.getId shouldBe psId.getValue
        p.person.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.person.getNameFirstRep.getFamily shouldBe "Doe2"
        p.person.getNameFirstRep.getGivenAsSingleString shouldBe "John"
        p.person.getIdentifierFirstRep.getValue shouldBe ramq

      case Invalid(_) => fail("Expect Valid")
    }
  }

  it should "return no errors if person exist for a patient with the same ndm and ep and with a ramq null in fhir resource" in {
    val ramqPatientEp1 = nextId()
    val ramqPatientEp2 = nextId()
    val ndm = nextId()
    Given("An existing organization")
    val ep1 = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")
    val ep2 = FhirTestUtils.loadOrganizations(id = "CHUM", name = "CHU Montreal", alias = "CHUM")

    And("And existing patient in fhir with a NDM and an organization")
    val ptIdEp1 = FhirTestUtils.loadPatients(managingOrganization = Some(ep1), identifier = Some(ndm))
    val ptIdEp2 = FhirTestUtils.loadPatients(managingOrganization = Some(ep2), identifier = Some(ndm)) //Same id but different EP

    And("An existing person in fhir associated to the patient")
    val psId = FhirTestUtils.loadPerson(ramq = None, patientId = Seq(ptIdEp1.getIdPart,ptIdEp2.getIdPart))
    And("A patient in metadata")
    val patientEp1 = defaultFullPatient(lastName = "Doe2", ramq = Some(ramqPatientEp1), ndm = Some(ndm), ep = ep1)
    val patientEp2 = defaultFullPatient(ramq = Some(ramqPatientEp2), ndm = Some(ndm), ep = ep2)
    When("Validate person")
    val resPersonEp1 = PersonValidation.validatePerson(patientEp1)

    Then("A valid person is returned")
    resPersonEp1 match {
      case Valid(TNewPerson(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPerson(_, _)) =>
        p.person.getId shouldBe psId.getValue
        p.person.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.person.getNameFirstRep.getFamily shouldBe "Doe2"
        p.person.getNameFirstRep.getGivenAsSingleString shouldBe "John"
        p.person.getIdentifierFirstRep.getValue shouldBe ramqPatientEp1

      case Invalid(_) => fail("Expect Valid")
    }

    val resPersonEp2 = PersonValidation.validatePerson(patientEp2)

    Then("A valid person is returned")
    resPersonEp2 match {
      case Valid(TNewPerson(_)) =>
        fail("Expect a TExistingPerson instance")
      case Valid(p@TExistingPerson(_, _)) =>
        p.person.getId shouldBe psId.getValue
        p.person.getGender.toCode shouldBe AdministrativeGender.MALE.toCode
        p.person.getNameFirstRep.getFamily shouldBe "Doe"
        p.person.getNameFirstRep.getGivenAsSingleString shouldBe "John"
        p.person.getIdentifierFirstRep.getValue shouldBe ramqPatientEp2

      case Invalid(_) => fail("Expect Valid")
    }

  }


  it should "return errors if person exist for a patient with the same ndm and ep but with a different ramq" in {
    val ramqFullPatient = nextId()
    val ramqExistingPerson = nextId()
    val ndm = nextId()
    Given("An existing organization")
    val orgId = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")

    And("And existing patient in fhir with a NDM and an organization")
    val ptId = FhirTestUtils.loadPatients(managingOrganization = Some(orgId), identifier = Some(ndm))

    And("An existing person in fhir associated to the patient")
    val psId = FhirTestUtils.loadPerson(ramq = Some(ramqExistingPerson), patientId = Seq(ptId.getIdPart))

    And("A patient in metadata, but with a different ramq than person in fhir")
    val patient = defaultFullPatient(lastName = "Doe2", ramq = Some(ramqFullPatient), ndm = Some(ndm), ep = orgId)

    When("Validate person")
    val res = PersonValidation.validatePerson(patient)

    Then("A valid person is returned")
    res.isValid shouldBe false
  }

  it should "return an error if no ramq and no ndm" in {
    When("Validate person")
    val res = PersonValidation.validatePerson(defaultFullPatient(ramq = None, ndm = None))

    Then("An error is returned")
    res.isValid shouldBe false
  }

}
