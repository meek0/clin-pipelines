package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.FhirTestUtils
import bio.ferlab.clin.etl.task.ldmnotifier.TasksGqlExtractor
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.{FlatSpec, GivenWhenThen}

class TasksGqlExtractorSpec extends FlatSpec with GivenWhenThen {
  "buildGqlTasksQueryHttpPostBody" should "give a correct http post body" in {
    Given("a runName (for example, 'abc') ")
    val runName = "abc"
    Then("a correct graphql query should be produced")
    val httpBody = TasksGqlExtractor.buildGqlTasksQueryHttpPostBody(runName)
    val formattedBody = httpBody
      .replace(" ","")
      .replace("\\n","")
      .replace("\\t","")
    println(formattedBody)
    formattedBody shouldBe """{"query":"{taskList:TaskList(run_name:\"abc\"){idowner@flatten{owner:resource(type:Organization){idalias@first@singletontelecom@flatten@first@singleton{email:value}}}output@flatten{valueReference@flatten{attachments:resource(type:DocumentReference){content@flatten{urls:attachment{url}}}}}}}"}""".stripMargin
  }

  "A well-formed graphql response with no data" should "be handled correctly" in {
    Given("a correctly parsed json containing no data ({ 'data': {} })")
    val parsed = FhirTestUtils.parseJsonFromResource("task/graphql_http_resp_empty_data_no_errors.json")
    val eitherErrorOrData = TasksGqlExtractor.checkIfGqlResponseHasData(parsed.get)
    Then("error is reported")
    assert(eitherErrorOrData.isLeft)
  }

  "A well-formed graphql response with data" should "give task(s)" in {
    Given("a correctly parsed json containing 4 tasks")
    val parsed = FhirTestUtils.parseJsonFromResource("task/graphql_http_resp_run_name_1.json")

    Then("the tasks should be extracted with no errors")
    val eitherErrorOrData = TasksGqlExtractor.checkIfGqlResponseHasData(parsed.get)
    eitherErrorOrData.isRight shouldBe true
  }
}

