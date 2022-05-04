package bio.ferlab.clin.etl.task.fileimport.model

import org.hl7.fhir.r4.model.IdType

case class FamilyExtension(patientId: IdType, code: String)

object FamilyExtension {
  def buildFamilies(patients: Seq[TPatient]): Map[String, Seq[FamilyExtension]] = {
    patients.filter(_.patient.familyId.isDefined)
      .groupBy(p => p.patient.familyId.get)
      .map { case (familyId, patients) =>
        familyId -> patients.collect { case p if p.patient.familyMember != "PROBAND" => FamilyExtension(p.id, p.patient.familyMember) }
      }
  }
}