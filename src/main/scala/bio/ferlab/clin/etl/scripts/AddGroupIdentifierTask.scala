package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Extensions
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.{Bundle, Identifier, StringType, Task}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

object AddGroupIdentifierTask {

  val LOGGER: Logger = LoggerFactory.getLogger(AddGroupIdentifierTask.getClass)

  def apply(fhirClient: IGenericClient, params: Array[String]): ValidationResult[Boolean] = {

    val dryRun = params.contains("--dryrun")

    // search for all tasks
    val tasksBundle = fhirClient.search().forResource(classOf[Task])
      .count(Int.MaxValue) // 20 by default
      .returnBundle(classOf[Bundle]).encodedJson().execute()

    // extract from bundle response
    val entries = tasksBundle.getEntry.asScala
    val tasks = entries.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH && !be.getResource.asInstanceOf[Task].hasGroupIdentifier => be.getResource.asInstanceOf[Task] }

    // list of resources we want to update
    val res: Seq[BundleEntryComponent] = tasks.foldLeft(Seq.empty[BundleEntryComponent]) { (acc, task) =>
      val runName = task
        .getExtensionByUrl(Extensions.SEQUENCING_EXPERIMENT)
        .getExtensionByUrl("runName")
        .getValue.asInstanceOf[StringType].getValue
      task.setGroupIdentifier(new Identifier().setValue(runName))
      acc ++ FhirUtils.bundleUpdate(Seq(task))
    }
    val bundle = TBundle(res.toList)
    LOGGER.info("Request:\n" + bundle.print()(fhirClient))

    if (dryRun) {
      Valid(true)
    } else {
      val result = bundle.save()(fhirClient)
      LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
      Valid(result.isValid)
    }

  }

}
