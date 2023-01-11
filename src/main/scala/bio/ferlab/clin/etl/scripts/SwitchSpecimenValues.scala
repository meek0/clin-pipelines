package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.{Bundle, Reference, ServiceRequest, Specimen}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

object SwitchSpecimenValues {

  val LOGGER: Logger = LoggerFactory.getLogger(SwitchSpecimenValues.getClass)

  def apply(fhirClient: IGenericClient, params: Array[String]): ValidationResult[Boolean] = {

    val dryRun = params.contains("--dryRun")

    // search for all sequencings
    val sequencingsBundle = fhirClient.search().forResource(classOf[ServiceRequest])
      .withProfile("http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-sequencing-request")
      .returnBundle(classOf[Bundle]).encodedJson().execute()

    // extract from bundle response
    val entries = sequencingsBundle.getEntry.asScala
    val sequencings = entries.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[ServiceRequest]}

    // list of resources we want to update
    var res: Seq[BundleEntryComponent] = Seq()

    sequencings.foreach(sequencing => {
      if (sequencing.getSpecimen.size() == 2) {
        val sp1Ref = sequencing.getSpecimen.get(0)
        val sp2Ref = sequencing.getSpecimen.get(1)

        // not optimized but simple and safe + keep order
        val sp1 = fhirClient.read().resource(classOf[Specimen]).withId(sp1Ref.getReference).encodedJson().execute()
        val sp2 = fhirClient.read().resource(classOf[Specimen]).withId(sp2Ref.getReference).encodedJson().execute()

        val sp1Value = sp1.getAccessionIdentifier.getValue
        val sp2Value = sp2.getAccessionIdentifier.getValue

        // following are the changes to apply
        sp1.getAccessionIdentifier.setValue(sp2Value)
        sp2.getAccessionIdentifier.setValue(sp1Value)

        LOGGER.info(s"Specimen of type ${sp1.getType.getCodingFirstRep.getDisplay} value: ${sp1Value} => ${sp1.getAccessionIdentifier.getValue}")
        LOGGER.info(s"Specimen of type ${sp2.getType.getCodingFirstRep.getDisplay} value: ${sp2Value} => ${sp2.getAccessionIdentifier.getValue}")

        if (sp1.hasParent) switchDisplay(sp1.getParentFirstRep, sp2Value, sp1Value)
        if (sp2.hasParent) switchDisplay(sp2.getParentFirstRep, sp1Value, sp2Value)

        switchDisplay(sp1Ref, sp1Value, sp2Value)
        switchDisplay(sp2Ref, sp2Value, sp1Value)

        // add to list of resources we want to update
        res = res ++ FhirUtils.bundleUpdate(Seq(sequencing, sp1, sp2))

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

  private def switchDisplay(ref: Reference, oldValue: String, newValue: String) = {
    val before = ref.getDisplay;
    ref.setDisplay(before.replace(oldValue, newValue))
    LOGGER.info(s"Reference display: ${before} => ${ref.getDisplay}")
  }
}
