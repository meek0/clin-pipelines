package bio.ferlab.clin.etl.task.fileimport.validation.full

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Code.{JHN, MR}
import bio.ferlab.clin.etl.fhir.FhirUtils.{firstEntryWithIdentifierCode, firstInBundle, matchIdentifierCode, matchPersonAndPatient}
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.fileimport.model.{FullPatient, TExistingPatient, TNewPatient, TPatient}
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.{Bundle, IdType, Patient, Person}

object PatientValidation {

  def validatePatient(patient: FullPatient)(implicit client: IClinFhirClient, fhirClient: IGenericClient): ValidationResult[TPatient] = {
    findPatientByNDM(patient).orElse(findPatientByRAMQ(patient)).map(p => TExistingPatient(patient, p)).getOrElse(TNewPatient(patient)).validateBaseResource()
  }

  private def findPatientByNDM(patient: FullPatient)(implicit client: IClinFhirClient, fhirClient: IGenericClient) = {
    patient.mrn.flatMap { ndm =>
      val results = fhirClient
        .search
        .forResource(classOf[Patient])
        .where(Patient.IDENTIFIER.exactly().code(ndm))
        .and(Patient.ORGANIZATION.hasId(patient.ep))
        .encodedJson
        .returnBundle(classOf[Bundle]).execute

      // We need to match person by their MR, in case identifier value has been found for another coding type identifier
      // It could be done in the query with matcher of type  => identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|446053
      // but HAPI server does not support this yet

      firstEntryWithIdentifierCode[Patient](SearchEntryMode.MATCH, MR, results)

    }

  }

  private def findPatientByRAMQ(patient: FullPatient)(implicit client: IClinFhirClient, fhirClient: IGenericClient) = {
    patient.ramq.flatMap { ramq =>
      val results = fhirClient
        .search
        .forResource(classOf[Person])
        .where(Person.IDENTIFIER.exactly().code(ramq))
        .where(Person.PATIENT.hasChainedProperty(Patient.ORGANIZATION.hasId(patient.ep)))
        .include(Person.INCLUDE_PATIENT)
        .encodedJson
        .returnBundle(classOf[Bundle]).execute

      firstInBundle[Patient](SearchEntryMode.INCLUDE, results) { pt =>
        pt.hasManagingOrganization &&
          new IdType(pt.getManagingOrganization.getReference).getIdPart == patient.ep &&
          firstInBundle[Person](SearchEntryMode.MATCH, results) { person => matchPersonAndPatient(person, pt) }
            .exists(person => matchIdentifierCode(JHN, person))
      }

    }

  }


}
