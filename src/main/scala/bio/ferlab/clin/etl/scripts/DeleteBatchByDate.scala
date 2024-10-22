package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.param.DateRangeParam
import cats.data.Validated.Valid
import org.apache.commons.lang3.StringUtils
import org.hl7.fhir.instance.model.api.IBaseResource
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model._
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters.asScalaBufferConverter

object DeleteBatchByDate {

  val LOGGER: Logger = LoggerFactory.getLogger(DeleteBatchByDate.getClass)

  def apply(fhirClient: IGenericClient, params: Array[String]): ValidationResult[Boolean] = {

    val dryRun = params.contains("--dryrun")
    val expectedBatchId = params(0)
    val date = params(1)

    LOGGER.info(s"Delete batch by date: $expectedBatchId $date (drynrun=$dryRun)")

    // SAFETY #1 to limit the risk we build our own max date from the param + 1 second
    // considering an import has always the same date for each resources 1 second is plenty
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    val datePlusOne = dateFormat.format(LocalDateTime.parse(date, dateFormat).plusSeconds(1))

    if (StringUtils.isAnyBlank(expectedBatchId, date, datePlusOne) || !date.contains("T") || !datePlusOne.contains("T")) {
      throw new IllegalArgumentException("Usage: <batch_id> <lastUpdated> --dryrun \nex: 240119_A00516_0506_BHN3J2DMXY_germinal.part1 2024-01-31T23:17:55 --dryrun")
    }

    val lastUpdatedParam = new DateRangeParam(date, datePlusOne)

    // a delete needs to respect the following order of deletions cause some resources reference others
    val documentReferences = fetchBundleFor(lastUpdatedParam, classOf[DocumentReference])(fhirClient)
    val taskReferences = fetchBundleFor(lastUpdatedParam, classOf[Task])(fhirClient)
    // specimens are re-used dont delete them
    //val specimenReferences = fetchBundleFor(lastUpdatedParam, classOf[Specimen])(fhirClient)
    val observationReferences = fetchBundleFor(lastUpdatedParam, classOf[Observation])(fhirClient)
    val clinicalImpressionReferences = fetchBundleFor(lastUpdatedParam, classOf[ClinicalImpression])(fhirClient)
    val serviceRequestReferences = fetchBundleFor(lastUpdatedParam, classOf[ServiceRequest])(fhirClient)

    // SAFETY #2 validate the task batch_id is the expected one
    taskReferences.foreach(ref => {
      val batchId = ref.getGroupIdentifier.getValue
      if (!batchId.equals(expectedBatchId)) {
        throw new IllegalArgumentException(s"Trying to delete something from another batch: ${batchId}")
      }
    })

    // SAFETY #3 log S3 files if we have to do something with them later
    documentReferences.foreach(ref => {
      ref.getContent.forEach(c => {
        val attachment = c.getAttachment
        LOGGER.info(s"DocumentReference: ${attachment.getUrl} => ${attachment.getTitle}")
      })
    })

    val bundle = TBundle(FhirUtils.bundleDelete(documentReferences ++ taskReferences ++ /*specimenReferences ++*/ observationReferences ++ clinicalImpressionReferences ++ serviceRequestReferences).toList)
    LOGGER.info("Request:\n" + bundle.print()(fhirClient))

    if (dryRun) {
      Valid(true)
    } else {
      val result = bundle.save()(fhirClient)
      LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
      Valid(result.isValid)
    }
  }

  private def fetchBundleFor[T <: IBaseResource](lastUpdatedParam: DateRangeParam, clazz: Class[T])(implicit fhirClient: IGenericClient) = {
    val res = fhirClient.search().forResource(clazz)
      .lastUpdated(lastUpdatedParam)
      .count(Int.MaxValue)
      .returnBundle(classOf[Bundle]).encodedJson().execute()
    res.getEntry.asScala.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[T] }
  }
}
