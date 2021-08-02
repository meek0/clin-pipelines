package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.task.fileimport.model.Analysis
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.IdType

import scala.collection.JavaConverters._

object OrganizationValidation {
  def validateOrganization(a: Analysis)(implicit client: IGenericClient): ValidationResult[IdType] = {
    validateOneOrganization(a.ldm)
  }

  def validateCQGCOrganization()(implicit client: IGenericClient): ValidationResult[IdType] = {
    validateOneOrganization("CQGC")
  }

  def validateOneOrganization(ldm: String)(implicit client: IGenericClient): ValidationResult[IdType] = {
    import ca.uhn.fhir.rest.gclient.StringClientParam
    import org.hl7.fhir.r4.model.{Bundle, Organization}
    val bundle: Bundle = client.search.
      forResource(classOf[Organization])
      .where(new StringClientParam("name")
        .matchesExactly
        .value(ldm)
      )
      .returnBundle(classOf[Bundle]).execute


    if (!bundle.hasEntry) {
      s"Organization ${ldm} does not exist".invalidNel[IdType]
    } else if (bundle.getTotal > 1) {
      val resources = bundle.getEntry.asScala.map(_.getId).mkString(", ")
      s"Multiples organizations found for ${ldm}: $resources".invalidNel[IdType]
    } else {
      val org = bundle.getEntryFirstRep.getResource
      IdType.of(org).validNel[String]
    }

  }
}
