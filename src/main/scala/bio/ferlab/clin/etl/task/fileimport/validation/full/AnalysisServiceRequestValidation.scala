package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.task.fileimport.model.{FullAnalysis, TAnalysisServiceRequest, TFullServiceRequest}
import ca.uhn.fhir.rest.client.api.IGenericClient

object AnalysisServiceRequestValidation {

  def validateAnalysisServiceRequest(a: FullAnalysis)(implicit client: IGenericClient): ValidationResult[TAnalysisServiceRequest] = {
    TAnalysisServiceRequest(a).validateBaseResource()
  }
}

