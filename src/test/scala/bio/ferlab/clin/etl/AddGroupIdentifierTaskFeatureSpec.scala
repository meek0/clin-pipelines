package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.scripts.AddGroupIdentifierTask
import bio.ferlab.clin.etl.task.fileimport.model.{Experiment, TTask, TaskExtensions}
import bio.ferlab.clin.etl.task.fileimport.validation.TaskExtensionValidation
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.defaultExperiment
import bio.ferlab.clin.etl.testutils.{FhirTestUtils, WholeStackSuite}
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model._
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.collection.JavaConverters._
import scala.io.Source

class AddGroupIdentifierTaskFeatureSpec extends FlatSpec with WholeStackSuite with Matchers {


  private def loadTask(runName: String, groupIdentifier: Option[String] = None): String = {
    val task1 = new Task()
    val experiment1 = defaultExperiment.copy(runName = Some(runName))
    val expExtension = TaskExtensionValidation.buildExperimentExtension(experiment1)
    groupIdentifier.foreach(gi => task1.setGroupIdentifier(new Identifier().setValue(gi)))
    task1.addExtension(expExtension)
    fhirClient.create().resource(task1).execute().getId.getIdPart

  }

  "run" should "return no errors" in {


    val task1Id = loadTask("run1")
    val task2Id = loadTask("run3")
    val task3Id = loadTask("run1", Some("bat1"))
    val result = AddGroupIdentifierTask(fhirClient, Array())
    result shouldBe Valid(true)
    val searchTasks = searchFhir("Task")
    searchTasks.getTotal shouldBe 3

    val entries = searchTasks.getEntry.asScala.map(_.getResource.asInstanceOf[Task])

    val task1Result = entries.find(t => IdType.of(t).getIdPart == task1Id)
    task1Result.get.getGroupIdentifier.getValue shouldBe "run1"

    val task2Result = entries.find(t => IdType.of(t).getIdPart == task2Id)
    task2Result.get.getGroupIdentifier.getValue shouldBe "run3"

    val task3Result = entries.find(t => IdType.of(t).getIdPart == task3Id)
    task3Result.get.getGroupIdentifier.getValue shouldBe "bat1" //should not be updated because groupIdentifier exist before running the task


  }


}