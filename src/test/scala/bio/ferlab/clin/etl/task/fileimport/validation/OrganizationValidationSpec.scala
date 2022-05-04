package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.testutils.{FhirServerSuite, FhirTestUtils}
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class OrganizationValidationSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {
  "validateOrganizationById" should "return invalid" in {
    FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")
    val res = OrganizationValidation.validateOrganizationById("LDM-CHUSJ")
    res.isValid shouldBe false

  }

  "validateOrganizationById" should "return valid" in {
    val orgId = FhirTestUtils.loadOrganizations(id = "CHUS", name = "CHU Sherbrooke", alias = "CHUS")
    val res = OrganizationValidation.validateOrganizationById(orgId)
    res match {
      case Valid(a) => a.getIdPart shouldBe orgId
      case Invalid(_) => fail("Expect valid result")
    }

  }

}
