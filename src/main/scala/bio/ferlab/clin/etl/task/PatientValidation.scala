package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.model.{Metadata, Patient}
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException
import org.hl7.fhir.r4.model.IdType

import scala.util.Try

object PatientValidation {

  def validate(metadata: Metadata)(implicit clinClient: IClinFhirClient): Seq[String] = {
    val patients: Seq[Patient] = metadata.analyses.map(_.patient)
    patients.flatMap { p =>
      val fhirPatient = Try(Some(clinClient.getPatientById(new IdType(p.id)))).recover {
        case _: ResourceNotFoundException => None
      }.get

      fhirPatient match {
        case None => Seq(s"Patient ${p.id} does not exist")
        case Some(fp) =>
          val firstName = fp.getNameFirstRep.getGivenAsSingleString
          val lastName = fp.getNameFirstRep.getFamily
          val sex = fp.getGender.getDisplay.toLowerCase
          Seq(
            matchProperty("First Name")(p.id, p.firstName, firstName),
            matchProperty("Last Name")(p.id, p.lastName, lastName),
            matchProperty("Sex")(p.id, p.sex.toLowerCase, sex),
            matchActive(p.id, fp.getActive),
          ).flatten
      }

    }
  }

  def matchActive(id: String, isActive: Boolean): Option[String] = {
    if (isActive) {
      None
    } else
      Some(s"Patient id=$id : patient is inactive")
  }

  def matchProperty[T](propertyName: String)(patientId: String, s1: T, s2: T): Option[String] = {
    if (s1 != s2) {
      Some(s"Patient id=$patientId : $propertyName are not the same ($s1 <-> $s2)")
    } else None
  }
}
