package bio.ferlab.clin.etl.task.fileimport.model

import org.hl7.fhir.r4.model.{IdType, Resource}

trait ExistingResource {
  def resource: Resource

  lazy val id: IdType = IdType.of(resource)
}