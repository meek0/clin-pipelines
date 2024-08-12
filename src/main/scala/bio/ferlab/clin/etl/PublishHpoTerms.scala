package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.es.{EsClient, EsHPO}
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import cats.data.Validated.Valid
import org.apache.commons.lang3.StringUtils
import org.hl7.fhir.r4.model.CodeSystem
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus
import org.slf4j.{Logger, LoggerFactory}

object PublishHpoTerms extends App {

  withSystemExit {
    withExceptions {
      withConf { conf =>
        withLog {
          val Array(index, release, alias, params @ _*) = args
          PublishHpoTerms(index, release, alias, params)(conf)
        }
      }
    }
  }

  def apply(index: String, release: String, alias: String, params : Seq[String])(implicit conf: Conf): ValidationResult[Any] = {
    val LOGGER: Logger = LoggerFactory.getLogger(getClass)

    val dryRun = params.contains("--dryrun")

    if (StringUtils.isAnyBlank(index, release, alias)) {
      throw new IllegalStateException("Expecting params: <index> <release> <alias>")
    }

    LOGGER.info(s"Publish HPO terms to FHIR from index: ${index}_${release} and alias: $alias (dryrun = $dryRun)")

    val esClient = new EsClient(conf.es)
    val (_, fhirClient) = buildFhirClients(conf.fhir, conf.keycloak)

    val esHPOs = esClient.getHPOs(index, release)

    if (esHPOs.isEmpty) {
      throw new IllegalStateException(s"No HPOs found in ES for index: ${index}_${release}")
    }

    LOGGER.info("Total ES HPOs available: " + esHPOs.size)

    if (!dryRun) {
      val fhirHPOs = generateFHIRCodeSystem(esHPOs)
      val bundle = TBundle(List(FhirUtils.bundleEntryUpdate(fhirHPOs)))
      //LOGGER.info("Request:\n" + bundle.print()(fhirClient))
      val result = bundle.save()(fhirClient)

      if (result.isInvalid) {
        return result
      }

      LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))

      esClient.publishHPO(index, release, alias)
    }

    Valid(true)
  }

  private def generateFHIRCodeSystem(hpos: Seq[EsHPO]): CodeSystem = {
    val code = new CodeSystem()

    code.setId("CodeSystem/hp")
    code.setUrl("http://purl.obolibrary.org/obo/hp.owl")
    code.setVersion("http://purl.obolibrary.org/obo/hp/releases/2020-03-27/hp.owl")
    code.setName("http://purl.obolibrary.org/obo/hp.owl")
    code.setTitle("Human Phenotype Ontology")
    code.setStatus(PublicationStatus.DRAFT)
    code.setExperimental(false)
    code.setDescription("Please see license of HPO at http://www.human-phenotype-ontology.org")
    code.setHierarchyMeaning(CodeSystem.CodeSystemHierarchyMeaning.ISA)
    code.setCompositional(false)
    code.setVersionNeeded(false)
    code.setContent(CodeSystem.CodeSystemContentMode.COMPLETE)
    code.setCount(hpos.size)

    hpos.foreach { hpo =>
      code.getConcept.add(new CodeSystem.ConceptDefinitionComponent().setCode(hpo.hpo_id).setDisplay(hpo.name).setDefinition(""))
    }

    code
  }

}
