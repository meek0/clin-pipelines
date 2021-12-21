package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.FhirTestUtils
import bio.ferlab.clin.etl.task.ldmnotifier.TasksGqlExtractor
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.{FlatSpec, GivenWhenThen}

class TasksGqlExtractorSpec extends FlatSpec with GivenWhenThen {
  it should "parse valid empty response" in {
    Given("a correctly parsed json containing no data ({ 'data': {} })")
    val parsed = FhirTestUtils.parseJsonFromResource("task/graphql_http_resp_empty_data_no_errors.json")
    val eitherErrorOrData = TasksGqlExtractor.checkIfGqlResponseHasData(parsed.get)
    Then("no error is reported")
    assert(eitherErrorOrData.isLeft)
  }

  it should "parse valid response containing data" in {
    Given("a correctly parsed json containing 3 tasks")
    val parsed = FhirTestUtils.parseJsonFromResource("task/graphql_http_resp_run_name_1.json")
    Then("the tasks should be extracted with no errors")
    val eitherErrorOrData = TasksGqlExtractor.checkIfGqlResponseHasData(parsed.get)
    eitherErrorOrData.isRight shouldBe true
    And("the extraction should lead to the correct number of Task object")
    eitherErrorOrData.right.get.size shouldBe 2
  }
}

