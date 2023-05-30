package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.SR_IDENTIFIER
import bio.ferlab.clin.etl.task.fileimport.model.{FullAnalysis, TFullServiceRequest, TSequencingServiceRequest}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits._
import org.hl7.fhir.r4.model.{Bundle, ServiceRequest}

object SequencingServiceRequestValidation {

  def validateSequencingServiceRequest(submissionSchema: Option[String], a: FullAnalysis)(implicit client: IGenericClient): ValidationResult[TSequencingServiceRequest] = {
    (validateExistingServiceRequest(a), TSequencingServiceRequest(submissionSchema, a).validateBaseResource()).mapN { case (_, r) => r }
  }

  private def validateExistingServiceRequest(a: FullAnalysis)(implicit client: IGenericClient) = {
    val results = client
      .search
      .forResource(classOf[ServiceRequest])
      .where(ServiceRequest.IDENTIFIER.exactly().systemAndCode(SR_IDENTIFIER, a.ldmServiceRequestId))
      .encodedJson
      .returnBundle(classOf[Bundle]).execute
    if (results.getTotal > 0) {
      s"Service request ${a.ldmServiceRequestId} already exist".invalidNel[Unit]
    } else ().validNel
  }

}
