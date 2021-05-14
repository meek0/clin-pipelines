package bio.ferlab.clin.etl.fhir

import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.server.exceptions.{PreconditionFailedException, UnprocessableEntityException}
import org.hl7.fhir.r4.model.{BooleanType, CodeType, DateType, DecimalType, Extension, IdType, IntegerType, OperationOutcome, Reference, Resource, StringType}

import scala.util.Try

object FhirUtils {

  object Constants {

    private val baseFhirServer = "http://fhir.cqgc.ferlab.bio"

    object CodingSystems {
      val SPECIMEN_TYPE = s"$baseFhirServer/CodeSystem/specimen-type"
      val DR_TYPE = s"$baseFhirServer/CodeSystem/data-type"
      val DR_CATEGORY = s"$baseFhirServer/CodeSystem/data-category"
      val DR_FORMAT = s"$baseFhirServer/CodeSystem/document-format"
      val EXPERIMENTAL_STRATEGY = s"$baseFhirServer/CodeSystem/experimental-strategy"
      val GENOME_BUILD = s"$baseFhirServer/CodeSystem/genome-build"
    }

    object Extensions {
      val WORKFLOW = s"$baseFhirServer/StructureDefinition/workflow"
      val SEQUENCING_EXPERIMENT = s"$baseFhirServer/StructureDefinition/sequencing-experiment"
    }

  }

  def validateResource(r: Resource)(implicit client: IGenericClient): OperationOutcome = {
    Try(client.validate().resource(r).execute().getOperationOutcome).recover {
      case e: PreconditionFailedException => e.getOperationOutcome
      case e: UnprocessableEntityException => e.getOperationOutcome
    }.get.asInstanceOf[OperationOutcome]
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
