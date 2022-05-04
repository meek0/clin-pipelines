package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Code.{JHN, MR}
import bio.ferlab.clin.etl.fhir.FhirUtils.{firstEntryWithIdentifierCode, firstInBundle, matchPersonAndPatient}
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.fileimport.model.{FullPatient, TExistingPerson, TNewPerson, TPerson}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits.catsSyntaxValidatedId
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.{Bundle, Patient, Person}

object PersonValidation {

  def validatePerson(patient: FullPatient)(implicit client: IClinFhirClient, fhirClient: IGenericClient): ValidationResult[TPerson] = {
    (patient.ramq, patient.mrn) match {
      case (None, None) => s"Patient: ${patient.firstName} ${patient.lastName} ${patient.birthDate} : At least one of ramq or ndm is required".invalidNel
      case _ =>
        val existingPerson = findPersonByRAMQ(patient)
          .orElse(findPersonByNDM(patient))
          .map(p => TExistingPerson(patient, p))
        existingPerson
          .getOrElse(TNewPerson(patient))
          .validateBaseResource()
    }
  }

  private def findPersonByRAMQ(patient: FullPatient)(implicit client: IClinFhirClient, fhirClient: IGenericClient) = {
    patient.ramq.flatMap { ramq =>
      val results = fhirClient
        .search
        .forResource(classOf[Person])
        .where(Person.IDENTIFIER.exactly().code(ramq))
        .encodedJson
        .returnBundle(classOf[Bundle]).execute
      // We need to match person by their JHN, in case identifier value has been found for another coding type identifier
      // It could be done in the query with matcher of type  => identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|JHN|446053
      // but HAPI server does not support this yet
      firstEntryWithIdentifierCode[Person](SearchEntryMode.MATCH, JHN, results)
    }
  }


  private def findPersonByNDM(patient: FullPatient)(implicit client: IClinFhirClient, fhirClient: IGenericClient) = {
    patient.mrn.flatMap { ndm =>
      val results = fhirClient
        .search
        .forResource(classOf[Patient])
        .where(Patient.IDENTIFIER.exactly().code(ndm))
        .and(Patient.ORGANIZATION.hasId(patient.ep))
        .encodedJson
        .revInclude(Person.INCLUDE_PATIENT)
        .returnBundle(classOf[Bundle]).execute

      // We need to match patient by their MR, in case identifier value has been found for another coding type identifier
      // It could be done in the query with matcher of type  => identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|446053
      // but HAPI server does not support this yet
      val resourcePatient = firstEntryWithIdentifierCode[Patient](SearchEntryMode.MATCH, MR, results)
      val person = resourcePatient.flatMap {
        p => firstInBundle[Person](SearchEntryMode.INCLUDE, results) { person => matchPersonAndPatient(person, p) }
      }
      person
    }
  }
}
