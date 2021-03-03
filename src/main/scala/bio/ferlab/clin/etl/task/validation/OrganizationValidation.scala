package bio.ferlab.clin.etl.task.validation
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.fhir.IClinFhirClient.opt
import bio.ferlab.clin.etl.model.Analysis
import ca.uhn.fhir.rest.param.StringParam
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.IdType

import scala.collection.JavaConverters._
object OrganizationValidation {
  def validateOrganization(a: Analysis)(implicit client: IClinFhirClient): ValidatedNel[String, IdType] = {
    val fhirOrg = opt(client.findByNameOrAlias(new StringParam(a.ldm)))
    fhirOrg match {
      case None => s"Organization ${a.ldm} does not exist".invalidNel[IdType]
      case Some(org) => IdType.of(org).validNel[String]
    }
  }
}
