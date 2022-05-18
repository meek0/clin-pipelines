package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Code.MR
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.IDENTIFIER_CODE_SYSTEM
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits._
import org.hl7.fhir.r4.model.{CodeableConcept, Coding, IdType, Patient, Reference}

import scala.collection.JavaConverters.asScalaBufferConverter


trait TPatient {
  def buildResource(organization: Reference): Patient = {
    resourcePatient.setId(id)
    resourcePatient.setManagingOrganization(organization)
    resourcePatient.getIdentifierFirstRep.setAssigner(organization)
    resourcePatient
  }

  def id: IdType

  protected def resourcePatient: Patient

  def patient: FullPatient

  def validateBaseResource()(implicit client: IGenericClient): ValidationResult[TPatient] = {
    (ndm(), administrativeGender()).mapN { (optNdm, gender) =>
      optNdm.foreach { r =>
        resourcePatient.addIdentifier()
          .setValue(r)
          .setType(new CodeableConcept().addCoding(
            new Coding().setSystem(IDENTIFIER_CODE_SYSTEM).setCode(MR))
          )
      }
      resourcePatient.setGender(gender)
      resourcePatient.setManagingOrganization(new Reference(s"Organization/${patient.ep}"))
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

  private def ndm() = FhirUtils.validateIdentifier(resourcePatient.getIdentifier, MR, "NDM", patient.mrn)
}

case class TExistingPatient(patient: FullPatient, resourcePatient: Patient) extends TPatient {
  override def id: IdType = IdType.of(resourcePatient)
}

case class TNewPatient(patient: FullPatient) extends TPatient {
  override val id: IdType = IdType.newRandomUuid()
  override val resourcePatient: Patient = {
    val p = new Patient()
    p
  }
}