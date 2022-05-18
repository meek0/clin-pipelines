package bio.ferlab.clin.etl.task.fileimport.model


import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Code.JHN
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.IDENTIFIER_CODE_SYSTEM
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.Person.PersonLinkComponent
import org.hl7.fhir.r4.model._

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.util.{Failure, Success, Try}


trait TPerson {
  def buildResource(patient: Reference): Person = {
    person.setId(id)
    person.addLink(new PersonLinkComponent(patient))
    person
  }

  protected def person: Person

  def patient: FullPatient

  def id: IdType

  def birthDate(): ValidatedNel[String, Date] = {
    Try(new SimpleDateFormat("dd/MM/yyyy").parse(patient.birthDate)) match {
      case Success(a: Date) =>
        a.validNel
      case Failure(e) =>
        e.getMessage.invalidNel
    }
  }

  def validateBaseResource()(implicit client: IGenericClient): ValidationResult[TPerson] = {
    (ramq(), administrativeGender(), birthDate()).mapN { (optRamq, gender, birthDate) =>
      optRamq.foreach { r =>
        person.addIdentifier()
          .setValue(r)
          .setType(new CodeableConcept().addCoding(
            new Coding().setSystem(IDENTIFIER_CODE_SYSTEM).setCode(JHN))
          )
      }
      person.getNameFirstRep
        .setFamily(patient.lastName)
        .setGiven(Seq(new StringType(patient.firstName)).asJava)
        .setUse(HumanName.NameUse.OFFICIAL)
      person.setGender(gender)
      person.setBirthDate(birthDate)
    }.andThen { p =>
      val outcomes = FhirUtils.validateResource(p)
      FhirUtils.validateOutcomes(outcomes, this) { o =>
        val diag = o.getDiagnostics
        val loc = o.getLocation.asScala.headOption.map(_.getValueNotNull).getOrElse("")
        s"Patient ${patient.firstName} ${patient.lastName} ${patient.birthDate} : $loc - $diag"
      }

    }

  }

  private def administrativeGender() = FhirUtils.validateAdministrativeGender(patient.sex)

  private def ramq() = FhirUtils.validateIdentifier(person.getIdentifier, JHN, "RAMQ", patient.ramq)

}

case class TExistingPerson(patient: FullPatient, person: Person) extends TPerson {
  override def id: IdType = IdType.of(person)

}

case class TNewPerson(patient: FullPatient) extends TPerson {
  override val id: IdType = IdType.newRandomUuid()
  override val person: Person = new Person()
}