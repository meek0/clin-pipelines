package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.isValid
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.server.exceptions.{PreconditionFailedException, UnprocessableEntityException}
import cats.data.ValidatedNel
import org.hl7.fhir.r4.model.{IdType, OperationOutcome, Reference, Resource}

import scala.collection.JavaConverters._
import scala.util.Try

object FhirUtils {

  object Constants {

    private val baseFhirServer = "http://fhir.cqgc.ferlab.bio"

    object CodingSystems {
      val SPECIMEN_TYPE = s"$baseFhirServer/CodeSystem/specimen-type"
      val DR_TYPE = s"$baseFhirServer/CodeSystem/data-type"
      val ANALYSIS_TYPE = s"$baseFhirServer/CodeSystem/analysis-type"
      val DR_CATEGORY = s"$baseFhirServer/CodeSystem/data-category"
      val DR_FORMAT = s"$baseFhirServer/CodeSystem/document-format"
      val EXPERIMENTAL_STRATEGY = s"$baseFhirServer/CodeSystem/experimental-strategy"
      val GENOME_BUILD = s"$baseFhirServer/CodeSystem/genome-build"
    }

    object Extensions {
      val WORKFLOW = s"$baseFhirServer/StructureDefinition/workflow"
      val SEQUENCING_EXPERIMENT = s"$baseFhirServer/StructureDefinition/sequencing-experiment"
      val FULL_SIZE = s"$baseFhirServer/StructureDefinition/full-size"
    }

  }

  def validateResource(r: Resource)(implicit client: IGenericClient): OperationOutcome = {
    Try(client.validate().resource(r).execute().getOperationOutcome).recover {
      case e: PreconditionFailedException => e.getOperationOutcome
      case e: UnprocessableEntityException => e.getOperationOutcome
    }.get.asInstanceOf[OperationOutcome]
  }


  def validateOutcomes[T](outcome: OperationOutcome, result: T)(err: (OperationOutcome.OperationOutcomeIssueComponent) => String): ValidatedNel[String, T] = {
    val issues = outcome.getIssue.asScala
    val errors = issues.collect {
      case o if o.getSeverity.ordinal() <= OperationOutcome.IssueSeverity.ERROR.ordinal => err(o)
    }
    isValid(result, errors)
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
      ref
    }


  }

  implicit class IdTypeExtension(v: IdType) {
    def toReference(): Reference = new Reference(v.toUnqualifiedVersionless)


  }

}
