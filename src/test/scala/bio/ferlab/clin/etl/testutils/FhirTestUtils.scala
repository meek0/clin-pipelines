package bio.ferlab.clin.etl.testutils

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.IDENTIFIER_CODE_SYSTEM
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.apache.commons.io.FileUtils
import org.hl7.fhir.instance.model.api.{IBaseResource, IIdType}
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender
import org.hl7.fhir.r4.model.Person.PersonLinkComponent
import org.hl7.fhir.r4.model._
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsValue, Json}

import java.io.File
import java.net.URL
import java.time.{LocalDate, ZoneId}
import java.util.{Collections, Date}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source
import scala.util.{Failure, Success, Try}

object FhirTestUtils {
  val DEFAULT_ZONE_ID: ZoneId = ZoneId.of("UTC")
  val ROOT_REMOTE_EXTENSION = "https://raw.githubusercontent.com/Ferlab-Ste-Justine/clin-fhir/master/site_root/input/resources/"
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def loadOrganizations(alias: String = "CHUSJ", name: String = "CHU Ste-Justine", id: String = "111")(implicit fhirClient: IGenericClient): String = {
    val org: Organization = new Organization()
    org.setId(id)
    org.setName(name)
    org.setAlias(Collections.singletonList(new StringType(alias)))

    val orgId: IIdType = fhirClient.create().resource(org).execute().getId
    LOGGER.info("Organization created with id : " + orgId.getIdPart)
    orgId.getIdPart
  }

  def loadCQGCOrganization()(implicit fhirClient: IGenericClient): String = {
    val cqgc: Organization = new Organization()
    cqgc.setId("CQGC")
    cqgc.setName("CQGC")
    cqgc.setAlias(Collections.singletonList(new StringType("CQGC")))

    val cqgcId: IIdType = fhirClient.update().resource(cqgc).execute().getId
    LOGGER.info("CQGC Organization created with id : " + cqgcId.getIdPart)
    cqgcId.getIdPart
  }

  def loadPerson(lastName: String = "Doe", firstName: String = "John", ramq: Option[String] = None, isActive: Boolean = true,
                 birthDate: LocalDate = LocalDate.of(2000, 12, 21),
                 gender: AdministrativeGender = Enumerations.AdministrativeGender.MALE,
                 patientId: Seq[String] = Nil
                )(implicit fhirClient: IGenericClient): IIdType = {
    val ps = new Person()
    ramq.foreach(r => ps.addIdentifier()
      .setValue(r)
      .setType(new CodeableConcept().addCoding(
        new Coding().setSystem(IDENTIFIER_CODE_SYSTEM).setCode("JHN").setDisplay("Jurisdictional health number (Canada)"))
      )
    )
    ps.setBirthDate(Date.from(birthDate.atStartOfDay(DEFAULT_ZONE_ID).toInstant))
    ps.setActive(isActive)
    ps.addName().setFamily(lastName).addGiven(firstName)
    ps.setGender(gender)
    patientId.foreach(ptId => ps.addLink(new PersonLinkComponent().setTarget(new Reference(s"Patient/$ptId"))))
    val id = fhirClient.create().resource(ps).execute().getId
    id
  }

  def loadPatients(lastName: String = "Doe", firstName: String = "John", identifier: Option[String] = Some("PT-000001"), isActive: Boolean = true,
                   birthDate: LocalDate = LocalDate.of(2000, 12, 21),
                   gender: AdministrativeGender = Enumerations.AdministrativeGender.MALE,
                   managingOrganization: Option[String] = None)(implicit fhirClient: IGenericClient): IIdType = {
    val pt: Patient = new Patient()
    identifier.foreach { id =>
      val id1: Resource = pt.setId(id)

      pt.addIdentifier()
        .setValue(id)
        .setType(new CodeableConcept().addCoding(new Coding().setCode("MR").setSystem(IDENTIFIER_CODE_SYSTEM)))
      pt.setIdElement(IdType.of(id1))
    }
    pt.setBirthDate(Date.from(birthDate.atStartOfDay(DEFAULT_ZONE_ID).toInstant))
    pt.setActive(isActive)
    pt.addName().setFamily(lastName).addGiven(firstName)
    pt.setGender(gender)
    pt.getMeta.addTag().setCode("test")

    managingOrganization.foreach(org => pt.setManagingOrganization(new Reference(s"Organization/$org")))
    val id = fhirClient.create().resource(pt).execute().getId

    LOGGER.info("Patient created with id : " + id.getIdPart)

    id
  }

  def loadServiceRequest(patientId: String, specimenIds: Seq[String] = Nil)(implicit fhirClient: IGenericClient): String = {
    val sr = new ServiceRequest()
    sr.setSubject(new Reference(s"Patient/$patientId"))
    sr.setStatus(ServiceRequest.ServiceRequestStatus.ACTIVE)
    sr.setIntent(ServiceRequest.ServiceRequestIntent.ORDER)
    specimenIds.foreach(spId => sr.addSpecimen(new Reference(s"Specimen/$spId")))
    val id: IIdType = fhirClient.create().resource(sr).execute().getId

    LOGGER.info("ServiceRequest created with id : " + id.getIdPart)
    id.getIdPart

  }

  def loadSpecimen(patientId: String, lab: String = "CHUSJ", submitterId: String = "1", specimenType: String = "NBL", parent: Option[String] = None, level: String = "specimen")(implicit fhirClient: IGenericClient): String = {
    val sp = new Specimen()
    sp.setSubject(new Reference(s"Patient/$patientId"))

    sp.getAccessionIdentifier.setSystem(s"https://cqgc.qc.ca/labs/$lab/$level").setValue(submitterId)

    sp.getType.addCoding()
      .setSystem("http://terminology.hl7.org/CodeSystem/v2-0487")
      .setCode(specimenType)
    parent.foreach { p =>

      sp.addParent(new Reference(s"Specimen/$p"))
    }
    val id: IIdType = fhirClient.create().resource(sp).execute().getId

    LOGGER.info("Specimen created with id : " + id.getIdPart)
    id.getIdPart

  }

  def findById[A <: IBaseResource](id: String, resourceType: Class[A])(implicit fhirClient: IGenericClient): Option[A] = {
    Option(
      fhirClient.read()
        .resource(resourceType)
        .withId(id)
        .execute()
    )
  }

  def clearAll()(implicit fhirClient: IGenericClient): Unit = {
    val inParams = new Parameters()
    inParams
      .addParameter().setName("expungePreviousVersions").setValue(new BooleanType(true))
    inParams
      .addParameter().setName("expungeDeletedResources").setValue(new BooleanType(true))
    Seq("Patient", "DocumentReference", "Organization", "Specimen", "Task", "ServiceRequest", "Person").foreach { r =>
      val t = fhirClient.delete()
        .resourceConditionalByUrl(s"$r?_lastUpdated=ge2017-01-01&_cascade=delete")
        .execute()

      println(s"Clean $r")
      fhirClient
        .operation()
        .onType(r)
        .named("$expunge")
        .withParameters(inParams)
        .execute()

    }


  }

  def init()(implicit fhirClient: IGenericClient): Unit = {
    def downloadAndCreate(p: String) = {
      val content = downloadIfNotInResources(p)
      fhirClient.create().resource(content).execute()
    }

    LOGGER.info("Init fhir container with extensions ...")

    //Sequential


    Seq(
      "terminology/CodeSystem-specimen-type.json",
      "terminology/CodeSystem-genome-build.json",
      "terminology/CodeSystem-experimental-strategy.json",
      "terminology/CodeSystem-document-format.json",
      "terminology/CodeSystem-data-type.json",
      "terminology/CodeSystem-data-category.json",
      "terminology/CodeSystem-bioinfo-analysis-code.json",
      "terminology/ValueSet-specimen-type.json",
      "terminology/ValueSet-genome-build.json",
      "terminology/ValueSet-data-type.json",
      "terminology/ValueSet-data-category.json",
      "terminology/ValueSet-age-at-onset.json").foreach(downloadAndCreate)
    Seq(
      "extensions/StructureDefinition-workflow.json",
      "extensions/StructureDefinition-sequencing-experiment.json",
      "extensions/StructureDefinition-full-size.json",
      "profiles/StructureDefinition-cqgc-analysis-task.json",
      "profiles/StructureDefinition-cqgc-observation.json",
      "profiles/StructureDefinition-cqgc-sequencing-request.json",
      "profiles/StructureDefinition-cqgc-analysis-request.json",
      "search/SearchParameter-run-name.json"

    ).foreach(downloadAndCreate)


  }

  def downloadIfNotInResources(p: String): String = {
    val resourceUrl = getClass.getResource(s"/fhir_extensions/$p")
    if (resourceUrl == null) {
      val remoteUrl = new URL(s"$ROOT_REMOTE_EXTENSION/$p")
      val resourcePath = s"${getClass.getResource("/").getPath}/fhir_extensions/$p"
      FileUtils.copyURLToFile(remoteUrl, new File(resourcePath))
      val source = Source.fromFile(resourcePath)
      val content = source.mkString
      source.close()
      content
    } else {
      val source = Source.fromURL(resourceUrl)
      val content = source.mkString
      source.close()
      content
    }
  }

  def parseJsonFromResource(resourceName: String): Try[JsValue] = {
    val source = Source.fromResource(resourceName)
    try {
      val strJson = source.mkString
      val parsedJson = Json.parse(strJson)
      Success(parsedJson)
    } catch {
      case e: Exception =>
        Failure(e)
    } finally {
      source.close()
    }
  }

  def getStringJsonFromResource(resourceName: String): Try[String] = {
    val source = Source.fromResource(resourceName)
    try {
      val strJson = source.mkString
      Success(strJson)
    } catch {
      case e: Exception =>
        Failure(e)
    } finally {
      source.close()
    }
  }
}


object testFhirUtils extends App {
  FhirTestUtils.downloadIfNotInResources("extensions/StructureDefinition-workflow.json")
}