package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.task.ldmnotifier.{TasksTransformer => Transformer}
import bio.ferlab.clin.etl.task.ldmnotifier.model.{Attachments, Owner, Task, Url}
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.{FlatSpec, GivenWhenThen}

class TasksTransformer extends FlatSpec with GivenWhenThen {
  def makeFakeTask(): Seq[Task] = {
    val Task1Ldm1 = Task(
      id = "task1",
      owner = Owner(id = "LDM1", alias = "LDM1", email = "LDM1@mail.com"),
      attachments = Attachments(urls = Seq(Url(url = "url1-LDM1/fileId1.tbi")))
    )
    val Task2Ldm1 = Task(
      id = "task2",
      owner = Owner(id = "LDM1", alias = "LDM1", email = "LDM1@mail.com"),
      attachments = Attachments(urls = Seq(Url(url = "url1-LDM1/fileId1.tbi"), Url(url = "url2-LDM1/fileId2.tbi")))
    )
    val Task3Ldm2 = Task(
      id = "task3",
      owner = Owner(id = "LDM2", alias = "LDM2", email = "LDM2@mail.com"),
      attachments = Attachments(urls = Seq(Url(url = "url1-LDM2/fileId1.tbi")))
    )
    val Task4Ldm3 = Task(
      id = "task4",
      owner = Owner(id = "LDM3", alias = "LDM3", email = "LDM3@mail.com"),
      attachments = Attachments(urls = Seq(Url(url = "url1-LDM3/fileId1.tbi"), Url(url = "url2-LDM3/fileId2.tbi")))
    )
    Seq(Task1Ldm1, Task2Ldm1, Task3Ldm2, Task4Ldm3)
  }

  "Attachments urls in tasks" should "be grouped by each of their aliases" in {
    Given("4 tasks having aliases and attachments (possibly with duplicates)")
    val tasks = makeFakeTask()

    Then("all attachments (more precisely, url values) should be grouped by each of their alias")
    val aliasToUrls = Transformer.groupAttachmentUrlsByEachOfOwnerAliases(tasks)
    aliasToUrls.isEmpty shouldBe false

    And("this very group should have the correct size")
    aliasToUrls.size shouldBe 3

    And("each alias should have the corresponding urls with no duplicates")
    aliasToUrls.get("LDM3") shouldBe Some(List("url1-LDM3/fileId1.tbi", "url2-LDM3/fileId2.tbi"))
    aliasToUrls.get("LDM2") shouldBe Some(List("url1-LDM2/fileId1.tbi"))
    aliasToUrls.get("LDM1") shouldBe Some(List("url1-LDM1/fileId1.tbi", "url2-LDM1/fileId2.tbi"))
  }

  "email addresses from tasks" should "be grouped by each of their aliases" in {
    Given("" +
      "4 tasks with email addresses and aliases" +
      " (assumes a one-to-one relationship between aliases and their respective email address)")
    val tasks = makeFakeTask()
    Then("a directory of type alias <-> email should be created with correct size")
    val aliasToEmailAddress = Transformer.mapLdmAliasToEmailAddress(tasks)
    aliasToEmailAddress.size shouldBe  3
    And("mapping should be correct")
    aliasToEmailAddress.get("LDM3") shouldBe Some("LDM3@mail.com")
    aliasToEmailAddress.get("LDM2") shouldBe Some("LDM2@mail.com")
    aliasToEmailAddress.get("LDM1") shouldBe Some("LDM1@mail.com")
  }
}