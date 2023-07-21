package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import ca.uhn.fhir.rest.api.SearchTotalModeEnum
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.{Bundle, Coding, Reference, ServiceRequest, Specimen}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._

import scala.collection.JavaConverters.asScalaBufferConverter

object MigrateServiceRequest {

  val LOGGER: Logger = LoggerFactory.getLogger(MigrateServiceRequest.getClass)

  def apply(fhirClient: IGenericClient, params: Array[String]): ValidationResult[Boolean] = {

    val dryRun = params.contains("--dryrun")

    // search for all sequencings
    val sequencingsBundle = fhirClient.search().forResource(classOf[ServiceRequest])
      .withProfile("http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-sequencing-request")
      .count(Int.MaxValue) // 20 by default
      .returnBundle(classOf[Bundle]).encodedJson().execute()

    // extract from bundle response
    val entries = sequencingsBundle.getEntry.asScala
    var sequencings = entries.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[ServiceRequest] }

    // list of resources we want to update
    var res: Seq[BundleEntryComponent] = Seq()
    sequencings.foreach(sequencing => {
      if (sequencing.getCode.getCoding.size() == 1) {
        var code = sequencing.getCode.getCoding.get(0).getCode
        if (!code.equals("TRATU") && !code.equals("EXTUM")) {
          val newCode = new Coding("http://fhir.cqgc.ferlab.bio/CodeSystem/sequencing-request-code", "75020", "Normal Exome Sequencing")
          sequencing.getCode.getCoding.add(newCode)
          res = res ++ FhirUtils.bundleUpdate(Seq(sequencing))
        }
      }
    })
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
