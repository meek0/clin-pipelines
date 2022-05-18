package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.task.fileimport.model.{FullAnalysis, TObservation}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel

object ObservationValidation {

  def validateObservation(a: FullAnalysis)(implicit client: IGenericClient): ValidatedNel[String, TObservation] = {
    TObservation(a).validateBaseResource()
  }
}

