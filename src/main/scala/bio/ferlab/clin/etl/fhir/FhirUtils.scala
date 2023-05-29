package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.IDENTIFIER_CODE_SYSTEM
import bio.ferlab.clin.etl.isValid
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.server.exceptions.{PreconditionFailedException, UnprocessableEntityException}
import cats.data.ValidatedNel
import cats.implicits.catsSyntaxValidatedId
import org.hl7.fhir.instance.model.api.IBaseResource
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender
import org.hl7.fhir.r4.model.{Bundle, IdType, Identifier, OperationOutcome, Patient, Person, Reference, Resource, Specimen}

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object FhirUtils {

  object Constants {

    private val baseFhirServer = "http://fhir.cqgc.ferlab.bio"
    object Code {
      val MR = "MR"
      val JHN = "JHN"
    }
    object CodingSystems {
      val SPECIMEN_TYPE = s"$baseFhirServer/CodeSystem/specimen-type"
      val DR_TYPE = s"$baseFhirServer/CodeSystem/data-type"
      val ANALYSIS_TYPE = s"$baseFhirServer/CodeSystem/bioinfo-analysis-code"
      val ANALYSIS_REQUEST_CODE = "http://fhir.cqgc.ferlab.bio/CodeSystem/analysis-request-code"
      val SEQUENCING_REQUEST_CODE = "http://fhir.cqgc.ferlab.bio/CodeSystem/sequencing-request-code"
      val DR_CATEGORY = s"$baseFhirServer/CodeSystem/data-category"
      val DR_FORMAT = s"$baseFhirServer/CodeSystem/document-format"
      val EXPERIMENTAL_STRATEGY = s"$baseFhirServer/CodeSystem/experimental-strategy"
      val GENOME_BUILD = s"$baseFhirServer/CodeSystem/genome-build"
      val OBJECT_STORE = "http://objecstore.cqgc.qc.ca"
      val IDENTIFIER_CODE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0203"
      val SR_IDENTIFIER = "https://cqgc.qc.ca/service-request"
      val FAMILY_IDENTIFIER = "https://cqgc.qc.ca/family"
      val OBSERVATION_INTERPRETATION = "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation"
      val OBSERVATION_CODE = "http://fhir.cqgc.ferlab.bio/CodeSystem/observation-code"
      val OBSERVATION_CATEGORY = "http://terminology.hl7.org/CodeSystem/observation-category"
    }

    object Extensions {
      val WORKFLOW = s"$baseFhirServer/StructureDefinition/workflow"
      val SEQUENCING_EXPERIMENT = s"$baseFhirServer/StructureDefinition/sequencing-experiment"
      val FULL_SIZE = s"$baseFhirServer/StructureDefinition/full-size"
    }
    object Profiles {
      val ANALYSIS_SERVICE_REQUEST = "http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-analysis-request"
      val SEQUENCING_SERVICE_REQUEST = "http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-sequencing-request"
      val OBSERVATION_PROFILE = "http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-observation"
    }

  }

  def firstEntry[T](bundle: Bundle, mode: SearchEntryMode): Option[T] = {
    val entries: Seq[Bundle.BundleEntryComponent] = bundle.getEntry.asScala
    entries.collectFirst { case be if be.getSearch.getMode == mode => be.getResource.asInstanceOf[T] }
  }

  type WithIdentifier = {def getIdentifier(): java.util.List[Identifier]}

  def firstEntryWithIdentifierCode[T <: WithIdentifier](mode: SearchEntryMode, code: String, bundle: Bundle): Option[T] = {
    firstInBundle[T](mode, bundle){
      r=> matchIdentifierCode(code, r)
    }
  }
  def matchPersonAndPatient(person:Person, patient:Patient): Boolean = {
    person.getLink.asScala.exists(linkComponent => {
      new IdType(linkComponent.getTarget.getReference).getIdPart == IdType.of(patient).getIdPart
    })
  }

  def matchIdentifierCode[T <: WithIdentifier](code: String, r: T): Boolean = {
    r.getIdentifier().asScala.exists(i => i.getType.hasCoding(IDENTIFIER_CODE_SYSTEM, code))
  }

  def firstInBundle[T](mode: SearchEntryMode, bundle: Bundle)(pred: T => Boolean): Option[T] = {
    bundle.getEntry.asScala.collect { case be if be.getSearch.getMode == mode =>
      val r = be.getResource.asInstanceOf[T]
      Some(r).filter(pred)
    }.flatten.headOption
  }

  def firstIncludeEntry[T](bundle: Bundle): Option[T] = firstEntry[T](bundle, SearchEntryMode.INCLUDE)

  def firstMatchEntry[T](bundle: Bundle): Option[T] = firstEntry[T](bundle, SearchEntryMode.MATCH)

  def validateResource(r: Resource)(implicit client: IGenericClient): OperationOutcome = {
    Try(client.validate().resource(r).execute().getOperationOutcome).recover {
      case e: PreconditionFailedException => e.getOperationOutcome
      case e: UnprocessableEntityException => e.getOperationOutcome
    }.get.asInstanceOf[OperationOutcome]
  }

  def validateAdministrativeGender(gender: String): ValidatedNel[String, AdministrativeGender] = {
    Try(AdministrativeGender.fromCode(gender.toLowerCase)) match {
      case Success(gender) => gender.validNel
      case Failure(_) => s"Invalid gender $gender".invalidNel
    }

  }

  def validateIdentifier(resourceIdentifier: java.util.List[Identifier], typeCode: String, fieldName: String, currentValue: Option[String]): ValidatedNel[String, Option[String]] = {
    val firstIdentifier = resourceIdentifier.asScala.collectFirst {
      case i if i.getType.getCoding.asScala.exists(c => c.getSystem == IDENTIFIER_CODE_SYSTEM && c.getCode == typeCode) => Option(i.getValue)
    }.flatten
    (currentValue, firstIdentifier) match {
      case (Some(a), Some(b)) if a != b => s"$fieldName are not the same ($a <-> $b)".invalidNel
      case (None, n@Some(_)) => n.validNel
      case (n, _) => n.validNel
    }
  }

  def validateOutcomes[T](outcome: OperationOutcome, result: T)(err: OperationOutcome.OperationOutcomeIssueComponent => String): ValidatedNel[String, T] = {
    val issues = outcome.getIssue.asScala
    val errors = issues.collect {
      case o if o.getSeverity.ordinal() <= OperationOutcome.IssueSeverity.ERROR.ordinal => err(o)
    }
    isValid(result, errors)
  }

  def bundleCreate(resources: Seq[Resource]): Seq[BundleEntryComponent] = resources.map {
    fhirResource =>
      val be = new BundleEntryComponent()
      be.setFullUrl(fhirResource.getIdElement.getValue)
        .setResource(fhirResource)
        .getRequest
        .setUrl(fhirResource.fhirType())
        .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.POST)
      be

  }

  def bundleUpdate(resources: Seq[Resource]): Seq[BundleEntryComponent] = resources.map {
    bundleEntryUpdate
  }

  def bundleEntryUpdate: Resource => BundleEntryComponent = {
    fhirResource =>
      val be = new BundleEntryComponent()
      be.setFullUrl(fullUrl(fhirResource))
        .setResource(fhirResource)
        .getRequest
        .setUrl(fullUrl(fhirResource))
        .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.PUT)
      be
  }

  def fullUrl(res: Resource) = {
    s"${res.getIdElement.getResourceType}/${res.getIdElement.getIdPart}"
  }

  def toJson(res: IBaseResource)(implicit client: IGenericClient) = {
    client.getFhirContext.newJsonParser.setPrettyPrint(true).encodeResourceToString(res)
  }

  def bundleDelete(resources: Seq[Resource]): Seq[BundleEntryComponent] = resources.map { fhirResource =>
    val be = new BundleEntryComponent()
    be
      .getRequest
      .setUrl(fhirResource.toReference().getReference)
      .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.DELETE)
    be
  }

  implicit class EitherResourceExtension(v: Either[IdType, Resource]) {
    def toReference(): Reference = {
      v match {
        case Right(r) => r.toReference()
        case Left(r) => r.toReference()
      }
    }

  }

  implicit class ResourceExtension(v: Resource) {
    def toReference(): Reference = {

      val ref = new Reference(IdType.of(v).toUnqualifiedVersionless)
      v.getResourceType.name() match {
        case "Specimen" =>
          val s = v.asInstanceOf[Specimen]
          val ldmId = s.getAccessionIdentifier.getValue
          val display = if (s.getParent == null || s.getParent.size() == 0) {
            s"Submitter Specimen ID: $ldmId"
          } else {
            s"Submitter Sample ID: $ldmId"
          }
          ref.setDisplay(display)
        case _ =>
      }
      ref
    }


  }

  implicit class IdTypeExtension(v: IdType) {
    def toReference(): Reference = new Reference(v.toUnqualifiedVersionless)


  }

}
