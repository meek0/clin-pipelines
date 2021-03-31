package bio.ferlab.clin.etl.task.validation

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.fhir.IClinFhirClient.opt
import bio.ferlab.clin.etl.model.InputPatient
import bio.ferlab.clin.etl.{ValidationResult, allValid}
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model.IdType
import cats.data.ValidatedNel
import cats.implicits._
import scala.collection.JavaConverters._

object PatientValidation {

  def validatePatient(p: InputPatient)(implicit clinClient: IClinFhirClient): ValidationResult[IdType] = {
    val fhirPatient = opt(clinClient.getPatientById(new IdType(p.id)))

    fhirPatient match {
      case None => s"Patient ${p.id} does not exist".invalidNel[IdType]
      case Some(fp) =>
        val firstName = fp.getNameFirstRep.getGivenAsSingleString
        val lastName = fp.getNameFirstRep.getFamily
        val sex = Option(fp.getGender).map(_.getDisplay).map(_.toLowerCase).getOrElse("")
        allValid(
          matchProperty("First Name")(p.id, p.firstName, firstName),
          matchProperty("Last Name")(p.id, p.lastName, lastName),
          matchProperty("Sex")(p.id, p.sex.toLowerCase, sex),
          matchActive(p.id, fp.getActive),
        )(IdType.of(fp))
    }
  }

  def matchActive(id: String, isActive: Boolean): ValidationResult[Any] = {
    if (isActive) {
      Valid()
    } else
      s"Patient id=$id : patient is inactive".invalidNel
  }

  def matchProperty[T](propertyName: String)(patientId: String, s1: T, s2: T): ValidationResult[Any] = {
    if (s1 != s2) {
      s"Patient id=$patientId : $propertyName are not the same ($s1 <-> $s2)".invalidNel
    } else Valid()
  }
}
