package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.fhir.Model._
import bio.ferlab.clin.etl.fhir.{AnalysisLoader, FileLoader, SpecimenLoader}
import org.hl7.fhir.instance.model.api.{IBaseResource, IIdType}
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender
import org.hl7.fhir.r4.model.Identifier.IdentifierUse
import org.hl7.fhir.r4.model._
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, ZoneId}
import java.util.{Collections, Date}

object FhirTestUtils {
  val DEFAULT_ZONE_ID = ZoneId.of("UTC")
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def loadOrganizations()(implicit fhirServer: FhirServerContainer) = {
    val org: Organization = new Organization()
    org.setId("111")
    org.setName("CHU Ste-Justine")
    org.setAlias(Collections.singletonList(new StringType("CHUSJ")))

    val id: IIdType = fhirServer.fhirClient.create().resource(org).execute().getId()
    LOGGER.info("Organization created with id : " + id.getValue())
    id.getValue()
  }

  def loadPractitioners()(implicit fhirServer: FhirServerContainer) = {
    val pr: Practitioner = new Practitioner()
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

    val id: IIdType = fhirServer.fhirClient.create().resource(pr).execute().getId()
    LOGGER.info("Practitioner created with id : " + id.getValue())
    id.getValue()
  }


  def loadPatients(lastName: String = "Doe", firstName: String = "John", identifier: String = "PT-000001", isActive: Boolean = true, birthDate: LocalDate = LocalDate.of(2000, 12, 21), gender: AdministrativeGender = Enumerations.AdministrativeGender.MALE)(implicit fhirServer: FhirServerContainer): String = {
    val pt: Patient = new Patient()
    val id1: Resource = pt.setId(identifier)
    pt.addIdentifier()
      .setSystem("http://terminology.hl7.org/CodeSystem/v2-0203")
      .setValue(identifier)
    pt.setBirthDate(Date.from(birthDate.atStartOfDay(DEFAULT_ZONE_ID).toInstant))
    pt.setActive(isActive)
    pt.addName().setFamily(lastName).addGiven(firstName)
    pt.setIdElement(IdType.of(id1))
    pt.setGender(gender)

    val id: IIdType = fhirServer.fhirClient.create().resource(pt).execute().getId()

    LOGGER.info("Patient created with id : " + id.getIdPart)
    id.getIdPart
  }

  def loadSpecimens(patientId: String, organisationId: String)(implicit fhirServer: FhirServerContainer): Seq[Specimen] = {


    val bodySite: Terminology = new Terminology("http://snomed.info/sct", "21483005", "Structure of central nervous system", Some("Central Nervous System"))
    val specimen: Specimen = SpecimenLoader.createSpecimen(fhirServer.clinClient, organisationId, patientId, ClinSpecimenType.BLOOD, bodySite)

    val saved: Specimen = fhirServer.fhirClient.create().resource(specimen).execute().getResource.asInstanceOf[Specimen]
    LOGGER.info("Specimen created with id : " + saved.getId())

    Seq(saved)
  }

  def loadSamples(patientId: String, organisationId: String, parents: Map[String, Specimen])(implicit fhirServer: FhirServerContainer): Seq[Specimen] = {

    val bodySite: Terminology = new Terminology("http://snomed.info/sct", "21483005", "Structure of central nervous system", Some("Central Nervous System"))
    val sample: Specimen = SpecimenLoader.createSpecimen(fhirServer.clinClient, organisationId, patientId, ClinSpecimenType.BLOOD, bodySite)
    val (key, value) = parents.head
    sample.setParent(Collections.singletonList(new Reference(value)))

    val saved: Specimen = fhirServer.fhirClient.create().resource(sample).execute().getResource.asInstanceOf[Specimen]
    LOGGER.info("Sample created with id : " + saved.getId())

    Seq(saved)
  }

  def loadFiles(patientId: String, organisationId: String, specimens: Seq[Specimen])(implicit fhirServer: FhirServerContainer): Seq[DocumentReference] = {
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

    val cram: DocumentReference = FileLoader.createFile(fhirServer.clinClient, "https://objectstore.cqgc.ca/", cramUUID, "current", "final",
      cramType, cramCategory, patientId, organisationId, cramFormat, cramInfo, Some(specimens), Some(alignmentMetrics), None)

    val savedCRAM: DocumentReference = fhirServer.fhirClient.create().resource(cram).execute().getResource.asInstanceOf[DocumentReference]
    LOGGER.info("CRAM created with id : " + savedCRAM.getId())

    //CRAI
    val craiType: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-types", "CRAMINDEX", "CRAM Index", None)
    val craiCategory: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-categories", "SNV", "Single Nucleotice Variation", None)
    val craiFormat: Terminology = new Terminology("http://fhir.cqgc.ferlab.bio/CodeSystem/document-formats", "CRAI", "CRAI", None)
    val craiInfo: FileInfo = new FileInfo("application/binary", s"http://objectstore.cqgc.ca/${craiUUID}", 2423234, "96e8b17925cal23jf23529efecc8f2", "pt1.crai", new Date())

    val crai: DocumentReference = FileLoader.createFile(fhirServer.clinClient, "https://objectstore.cqgc.ca/", craiUUID, "current", "final",
      craiType, craiCategory, "3", "1", craiFormat, craiInfo, None, None, Some(Map(savedCRAM -> "transform")))

    val savedCRAI: DocumentReference = fhirServer.fhirClient.create().resource(crai).execute().getResource.asInstanceOf[DocumentReference]
    LOGGER.info("CRAI created with id : " + savedCRAI.getId())

    Seq(savedCRAM, savedCRAI)
  }

  def loadAnalyses(content: Seq[DocumentReference])(implicit fhirServer: FhirServerContainer): Seq[DocumentManifest] = {
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

    val analysisWithoutRelatedAnalysis: DocumentManifest = AnalysisLoader.createAnalysis(fhirServer.clinClient, "3", "current", AnalysisType.SEQUENCING_ALIGNMENT, content, workflows, sequencingExperiments, Some("Json Text"), None)
    val savedAnalysisWithoutRelatedAnalysis: DocumentManifest = fhirServer.fhirClient.create().resource(analysisWithoutRelatedAnalysis).execute().getResource.asInstanceOf[DocumentManifest]
    LOGGER.info("Analysis created with id : " + savedAnalysisWithoutRelatedAnalysis.getId())

    val analysisWithRelatedAnalysis: DocumentManifest = AnalysisLoader.createAnalysis(fhirServer.clinClient, "3", "current", AnalysisType.SEQUENCING_ALIGNMENT, content, workflows, sequencingExperiments, Some("Json Text"), Some(Map(savedAnalysisWithoutRelatedAnalysis -> AnalysisRelationship.PARENT)))
    val savedAnalysisWithRelatedAnalysis: DocumentManifest = fhirServer.fhirClient.create().resource(analysisWithRelatedAnalysis).execute().getResource.asInstanceOf[DocumentManifest]
    LOGGER.info("Analysis with related analysis created with id : " + savedAnalysisWithRelatedAnalysis.getId())

    Seq(savedAnalysisWithoutRelatedAnalysis, savedAnalysisWithRelatedAnalysis)
  }

  def findById[A <: IBaseResource](id: String, resourceType: Class[A])(implicit fhirServer: FhirServerContainer): Option[A] = {
    Option(
      fhirServer.fhirClient.read()
        .resource(resourceType)
        .withId(id)
        .execute()
    )
  }

  def clearAll()(implicit fhirServer: FhirServerContainer) = {
    val inParams = new Parameters()
    inParams.addParameter.setName("expungeEverything").setValue(new BooleanType(true))
    fhirServer.fhirClient
      .operation()
      .onServer()
      .named("$expunge")
      .withParameters(inParams)
      .execute()

  }

  def printJson[A <: IBaseResource](resource: A)(implicit fhirServer: FhirServerContainer) = {
    LOGGER.info(fhirServer.parser.encodeResourceToString(resource))
  }
}
