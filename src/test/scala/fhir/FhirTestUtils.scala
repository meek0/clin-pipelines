package fhir

import java.util.{Collections, Date}

import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import fhir.Model.{AnalysisType, _}
import org.hl7.fhir.instance.model.api.{IBaseResource, IIdType}
import org.hl7.fhir.r4.model.Identifier.IdentifierUse
import org.hl7.fhir.r4.model._
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.shaded.org.apache.commons.lang.time.DateUtils

class FhirTestUtils(val fhirBaseUrl: String) {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  val fhirContext: FhirContext = FhirContext.forR4()
  fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
  fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)

  val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], fhirBaseUrl)
  val fhirClient: IGenericClient = fhirContext.newRestfulGenericClient(fhirBaseUrl)

  val parser: IParser = fhirContext.newJsonParser().setPrettyPrint(true)

  def loadOrganizations() = {
    val org: Organization = new Organization()
    org.setId("111")
    org.setName("CHU Ste-Justine")
    org.setAlias(Collections.singletonList(new StringType("CHUSJ")))

    val id:IIdType = fhirClient.create().resource(org).execute().getId()
    LOGGER.info("Organization created with id : " + id.getValue())
  }

  def loadPractitioners() = {
    val pr:Practitioner = new Practitioner()
    pr.setId("222")

    val cc: CodeableConcept = new CodeableConcept()
    val coding: Coding = new Coding()
    coding.setCode("MD")
    coding.setSystem("http://terminology.hl7.org/CodeSystem/v2-0203")
    coding.setDisplay("Medical License Number")
    cc.setText("Quebec Medical License Number")
    cc.addCoding(coding)

    pr.addIdentifier()
      .setSystem("http://terminology.hl7.org/CodeSystem/v2-0203")
      .setValue("PR-000001")
      .setUse(IdentifierUse.OFFICIAL)
      .setType(cc)

    pr.addName().setFamily("Afritt").addGiven("Barack").addPrefix("Dr.")

    val id:IIdType = fhirClient.create().resource(pr).execute().getId()
    LOGGER.info("Practitioner created with id : " + id.getValue())
  }

  def loadPatients() = {
    val pt:Patient = new Patient()
    pt.setId("333")
    pt.addIdentifier()
      .setSystem("http://terminology.hl7.org/CodeSystem/v2-0203")
      .setValue("PT-000001")
    pt.setBirthDate(DateUtils.addDays(new Date(), -8000))
    pt.setActive(true)
    pt.addName().setFamily("Bambois").addGiven("Jean")
    pt.setIdElement(IdType.newRandomUuid())
    pt.setGender(Enumerations.AdministrativeGender.MALE)

    val id:IIdType = fhirClient.create().resource(pt).execute().getId()
    LOGGER.info("Patient created with id : " + id.getValue())
  }

  def loadSpecimens(): Seq[Specimen] = {
    val org: Organization = clinClient.getOrganizationById(new IdType("1"))
    val pt: Patient = clinClient.getPatientById(new IdType("3"))

    val bodySite: Terminology = new Terminology("http://snomed.info/sct", "21483005", "Structure of central nervous system", Some("Central Nervous System"))
    val specimen: Specimen = SpecimenLoader.createSpecimen(clinClient, "1", "3", ClinSpecimenType.BLOOD, bodySite)

    val saved: Specimen = fhirClient.create().resource(specimen).execute().getResource.asInstanceOf[Specimen]
    LOGGER.info("Specimen created with id : " + saved.getId())

    Seq(saved)
  }

  def loadSamples(parents: Map[String, Specimen]): Seq[Specimen] = {
    val org: Organization = clinClient.getOrganizationById(new IdType("1"))
    val pt: Patient = clinClient.getPatientById(new IdType("3"))

    val bodySite: Terminology = new Terminology("http://snomed.info/sct", "21483005", "Structure of central nervous system", Some("Central Nervous System"))
    val sample: Specimen = SpecimenLoader.createSpecimen(clinClient, "1", "3", ClinSpecimenType.BLOOD, bodySite)
    val (key, value) = parents.head
    sample.setParent(Collections.singletonList(new Reference(value)))

    val saved: Specimen = fhirClient.create().resource(sample).execute().getResource.asInstanceOf[Specimen]
    LOGGER.info("Sample created with id : " + saved.getId())

    Seq(saved)
  }

  def loadFiles(specimens: Seq[Specimen]): Seq[DocumentReference] = {
    //CRAM
    val cramUUID: String = IdType.newRandomUuid().toString
    val craiUUID: String = IdType.newRandomUuid().toString

    val cramType: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-types", "AR", "Aligned Read", None)
    val cramCategory: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-categories", "SR", "Sequenced Read", None)
    val cramFormat: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-formats", "CRAM", "CRAM", None)
    val cramInfo: FileInfo = new FileInfo("application/binary", s"http://objectstore.cqgc.ca/${cramUUID}", 2423234, "96e8b17925cb65f9e5ca4f99efecc8f2", "pt1.cram", new Date())
    val alignmentMetrics: Seq[ClinExtension] = Seq(
      ClinExtension("TotalReads", "149737012", ClinExtensionValueType.INTEGER),
      ClinExtension("%PassFilterReads", "1.0", ClinExtensionValueType.DECIMAL),
      ClinExtension("%PassFilterUniqueReads", "0.76", ClinExtensionValueType.DECIMAL)
    )

    val cram: DocumentReference = FileLoader.createFile(clinClient, "https://objectstore.cqgc.ca/", cramUUID, "current", "final",
      cramType, cramCategory, "3", "1", cramFormat, cramInfo, Some(specimens), Some(alignmentMetrics), None)

    val savedCRAM: DocumentReference = fhirClient.create().resource(cram).execute().getResource.asInstanceOf[DocumentReference]
    LOGGER.info("CRAM created with id : " + savedCRAM.getId())

    //CRAI
    val craiType: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-types", "CRAMINDEX", "CRAM Index", None)
    val craiCategory: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-categories", "SNV", "Single Nucleotice Variation", None)
    val craiFormat: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-formats", "CRAI", "CRAI", None)
    val craiInfo: FileInfo = new FileInfo("application/binary", s"http://objectstore.cqgc.ca/${craiUUID}", 2423234, "96e8b17925cal23jf23529efecc8f2", "pt1.crai", new Date())

    val crai: DocumentReference = FileLoader.createFile(clinClient, "https://objectstore.cqgc.ca/", craiUUID, "current", "final",
      craiType, craiCategory, "3", "1", craiFormat, craiInfo, None, None, Some(Map(savedCRAM -> "transform")))

    val savedCRAI: DocumentReference = fhirClient.create().resource(crai).execute().getResource.asInstanceOf[DocumentReference]
    LOGGER.info("CRAI created with id : " + savedCRAI.getId())

    Seq(savedCRAM, savedCRAI)
  }

  def loadAnalyses(content: Seq[DocumentReference]): Seq[DocumentManifest] = {
    val workflows: Seq[ClinExtension] = Seq(
      ClinExtension("workflowName", "Dragen", ClinExtensionValueType.STRING),
      ClinExtension("workflowVersion", "1.1.0", ClinExtensionValueType.STRING),
      ClinExtension("genomeBuild", "GRCh38", ClinExtensionValueType.STRING),
      ClinExtension("variantClass", "somatic", ClinExtensionValueType.STRING)
    )

    val sequencingExperiments: Seq[ClinExtension] = Seq(
      ClinExtension("platform", "Illumina", ClinExtensionValueType.STRING),
      ClinExtension("sequencerID", "NB552318", ClinExtensionValueType.STRING),
      ClinExtension("runName", "runNameExample", ClinExtensionValueType.STRING),
      ClinExtension("runDate", "2014-09-21T11:50:23-05:00", ClinExtensionValueType.DATE),
      ClinExtension("isPairedEnd", "true", ClinExtensionValueType.BOOLEAN),
      ClinExtension("fragmentSize", "100", ClinExtensionValueType.INTEGER),
      ClinExtension("experimentalStrategy", "WXS", ClinExtensionValueType.CODE)
    )

    val analysisWithoutRelatedAnalysis: DocumentManifest = AnalysisLoader.createAnalysis(clinClient, "3", "current", AnalysisType.SEQUENCING_ALIGNMENT, content, workflows, sequencingExperiments, Some("Json Text"), None)
    val savedAnalysisWithoutRelatedAnalysis: DocumentManifest = fhirClient.create().resource(analysisWithoutRelatedAnalysis).execute().getResource.asInstanceOf[DocumentManifest]
    LOGGER.info("Analysis created with id : " + savedAnalysisWithoutRelatedAnalysis.getId())

    val analysisWithRelatedAnalysis: DocumentManifest = AnalysisLoader.createAnalysis(clinClient, "3", "current", AnalysisType.SEQUENCING_ALIGNMENT, content, workflows, sequencingExperiments, Some("Json Text"), Some(Map(savedAnalysisWithoutRelatedAnalysis -> AnalysisRelationship.PARENT)))
    val savedAnalysisWithRelatedAnalysis: DocumentManifest = fhirClient.create().resource(analysisWithRelatedAnalysis).execute().getResource.asInstanceOf[DocumentManifest]
    LOGGER.info("Analysis with related analysis created with id : " + savedAnalysisWithRelatedAnalysis.getId())

    Seq(savedAnalysisWithoutRelatedAnalysis, savedAnalysisWithRelatedAnalysis)
  }

  def findById[A <: IBaseResource](id: String, resourceType: Class[A]):Option[A] = {
    Option(
      fhirClient.read()
      .resource(resourceType)
      .withId(id)
      .execute()
    )
  }

  def printJson[A <: IBaseResource](resource: A) = {
    LOGGER.info(parser.encodeResourceToString(resource))
  }
}