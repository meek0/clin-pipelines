package bio.ferlab.clin.etl.task.validation

import bio.ferlab.clin.etl.model.Analysis
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.IdType

import scala.collection.JavaConverters._

object OrganizationValidation {
  def validateOrganization(a: Analysis)(implicit client: IGenericClient): ValidatedNel[String, IdType] = {
    import ca.uhn.fhir.rest.gclient.StringClientParam
    import org.hl7.fhir.r4.model.{Bundle, Organization}
    val bundle: Bundle = client.search.
      forResource(classOf[Organization])
      .where(new StringClientParam("name")
        .matchesExactly
        .value(a.ldm)
      )
      .returnBundle(classOf[Bundle]).execute


    if (!bundle.hasEntry) {
      s"Organization ${a.ldm} does not exist".invalidNel[IdType]
    } else if (bundle.getTotal > 1) {
      val resources = bundle.getEntry.asScala.map(_.getId).mkString(", ")
      s"Multiples organizations found for ${a.ldm}: $resources".invalidNel[IdType]
    }else {
      val org  = bundle.getEntryFirstRep.getResource
      IdType.of(org).validNel[String]
    }
//
//    fhirOrg match {
//      case None => s"Organization ${a.ldm} does not exist".invalidNel[IdType]
//      case Some(orgs) =>
//        orgs.asScala.toList match {
//          case Nil => s"Organization ${a.ldm} does not exist".invalidNel[IdType]
//          case org :: Nil => IdType.of(org).validNel[String]
//          case multipleOrgs =>
//            val resources = multipleOrgs.map(o =>
//              o.getId
//            ).mkString(", ")
//
//        }
//
//
//    }
  }
}
