package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.task.fileimport.model.TaskExtensions
import bio.ferlab.clin.etl.testutils.FhirServerSuite
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.{defaultExperiment, defaultMetadata, defaultWorkflow}
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.Inside.inside
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class TaskExtensionValidationSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {

  "validate" should "return no errors with correct metadata (contains run date)" in {
    TaskExtensionValidation.validateTaskExtension(defaultMetadata) should matchPattern {
      case Valid(TaskExtensions(_, _)) =>
    }
  }

  it should "return no errors with correct metadata (does not contain run date)" in {
    val metadata = defaultMetadata.copy(experiment = defaultExperiment.copy(runDate = None))
    TaskExtensionValidation.validateTaskExtension(metadata) should matchPattern {
      case Valid(TaskExtensions(_, _)) =>
    }
  }

  it should "return errors if run date is not valid" in {
    val metadata = defaultMetadata.copy(experiment = defaultExperiment.copy(runDate = Some("unparseable date")))
    inside(TaskExtensionValidation.validateTaskExtension(metadata)) {
      case Invalid(NonEmptyList(error, Nil)) =>
        error shouldBe "Error on experiment.rundate = unparseable date"
    }
  }

  it should "return errors if run date is valid with format dd/mm/yyyy" in {
    val metadata = defaultMetadata.copy(experiment = defaultExperiment.copy(runDate = Some("01/12/2006")))
    TaskExtensionValidation.validateTaskExtension(metadata) should matchPattern {
      case Valid(TaskExtensions(_, _)) =>
    }
  }

  it should "return errors if genomeBuild is not valid" in {
    val metadata = defaultMetadata.copy(workflow = defaultWorkflow.copy(genomeBuild = Some("invalid")))
    inside(TaskExtensionValidation.validateTaskExtension(metadata)) {
      case Invalid(NonEmptyList(error, Nil)) =>
        error should include("Unknown code")
        error should include("invalid")
    }
  }

  it should "return errors if experimentalStrategy is not valid" in {
    val metadata = defaultMetadata.copy(experiment = defaultExperiment.copy(experimentalStrategy = Some("unknown")))
    inside(TaskExtensionValidation.validateTaskExtension(metadata)) {
      case Invalid(NonEmptyList(error, Nil)) =>
        error should include("Unknown code")
    }
  }

  it should "return multiple errors" in {
    val metadata = defaultMetadata.copy(workflow = defaultWorkflow.copy(genomeBuild = Some("invalid")), experiment = defaultExperiment.copy(experimentalStrategy = Some("unknown")))
    inside(TaskExtensionValidation.validateTaskExtension(metadata)) {
      case Invalid(errors) =>
        errors.size shouldBe 2
    }
  }
}
