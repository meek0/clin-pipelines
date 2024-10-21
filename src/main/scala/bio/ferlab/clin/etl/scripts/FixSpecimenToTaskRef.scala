package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.scripts.FixFerloadURLs.LOGGER
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.server.exceptions.{ResourceGoneException, ResourceNotFoundException}
import org.apache.commons.lang3.StringUtils
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.hl7.fhir.r4.model.{Bundle, DocumentReference, Reference, ServiceRequest, Specimen, Task}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FixSpecimenToTaskRef {

  def apply(fhirClient: IGenericClient, params: Array[String]): Unit = {

    val dryRun = params.contains("--dryrun")
    val expectedBatchId = params(0)

    if (StringUtils.isAnyBlank(expectedBatchId) || expectedBatchId.equals("--dryrun")) {
      throw new IllegalArgumentException("Usage: <batch_id> --dryrun")
    }

    val allTasks = fhirClient.search().forResource(classOf[Task])
      .count(Int.MaxValue)
      .where(Task.GROUP_IDENTIFIER.exactly().code(expectedBatchId))
      .encodedJson()
      .returnBundle(classOf[Bundle]).execute()

    var res: Seq[BundleEntryComponent] = Seq()

    if (allTasks.getEntry.size() > 0) {
      allTasks.getEntry.forEach(entry => {
        val task = entry.getResource.asInstanceOf[Task]
        val taskServiceRequestId = task.getFocus.getReference
        task.getInput.forEach(input => {
          val taskSpecimenRef = input.getValue.asInstanceOf[Reference].getReference
          val specimen = fhirClient.read().resource(classOf[Specimen]).withId(taskSpecimenRef).encodedJson().execute()
          val specimenServiceRequestId = specimen.getRequestFirstRep.getReference
          if (!taskServiceRequestId.equals(specimenServiceRequestId)) {
            LOGGER.info(s"Task: ${task.getIdElement.getIdPart} and Specimen: ${specimen.getIdElement.getIdPart} ServiceRequest is different: $taskServiceRequestId != $specimenServiceRequestId")
            try {
              // validate the service request is indeed missing
              fhirClient.read().resource(classOf[ServiceRequest]).withId(specimenServiceRequestId).encodedJson().execute()
              LOGGER.warn(s"ServiceRequest: $specimenServiceRequestId found (The script doesn't handle such situation)")
            } catch {
              case e: ResourceGoneException =>
                LOGGER.error(s"ServiceRequest: $specimenServiceRequestId confirmed deleted at some point in time")
                specimen.getRequestFirstRep.setReference(taskServiceRequestId)
                res = res ++ FhirUtils.bundleUpdate(Seq(specimen))
            }
          } else {
            LOGGER.info(s"Task: ${task.getIdElement.getIdPart} and Specimen: ${specimen.getIdElement.getIdPart} ServiceRequest is the same : $taskServiceRequestId")
          }
        })
      })
    }

    if (!dryRun && res.nonEmpty) {
      val bundle = TBundle(res.toList)
      LOGGER.info("Request:\n" + bundle.print()(fhirClient))
      val result = bundle.save()(fhirClient)
      if (result.isValid) {
        LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
      }
    } else {
      LOGGER.info("No changes to apply")
    }
  }

}
