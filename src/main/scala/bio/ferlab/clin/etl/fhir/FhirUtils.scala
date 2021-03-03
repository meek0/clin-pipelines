package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.Model.{ClinExtension, ClinExtensionValueType}
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.server.exceptions.{PreconditionFailedException, UnprocessableEntityException}
import org.hl7.fhir.r4.model.{BooleanType, CodeType, DateType, DecimalType, Extension, IdType, IntegerType, OperationOutcome, Reference, Resource, StringType}

import scala.util.Try

object FhirUtils {

  def validateResource(r: Resource)(implicit client: IGenericClient): OperationOutcome = {
    Try(client.validate().resource(r).execute().getOperationOutcome).recover {
      case e: PreconditionFailedException => e.getOperationOutcome
      case e: UnprocessableEntityException => e.getOperationOutcome
    }.get.asInstanceOf[OperationOutcome]
  }

  def createExtension(url: String, values: Seq[ClinExtension]): Extension = {
    val ext:Extension = new Extension()
    ext.setUrl(url)

    values.foreach(metric => {
      metric.valueType match {
        case ClinExtensionValueType.DECIMAL => {
          ext.addExtension(metric.url, new DecimalType(metric.value))
        }
        case ClinExtensionValueType.INTEGER => {
          ext.addExtension(metric.url, new IntegerType(metric.value))
        }
        case ClinExtensionValueType.BOOLEAN => {
          ext.addExtension(metric.url, new BooleanType(metric.value))
        }
        case ClinExtensionValueType.DATE => {
          ext.addExtension(metric.url, new DateType(metric.value))
        }
        case ClinExtensionValueType.STRING => {
          ext.addExtension(metric.url, new StringType(metric.value))
        }
        case ClinExtensionValueType.CODE => {
          ext.addExtension(metric.url, new CodeType(metric.value))
        }
        case _ => {
          ext.addExtension(metric.url, new StringType(metric.value))
        }
      }
    })

    ext
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
