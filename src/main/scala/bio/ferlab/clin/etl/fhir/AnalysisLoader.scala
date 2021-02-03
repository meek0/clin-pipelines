package bio.ferlab.clin.etl.fhir

import java.util.Date

import org.hl7.fhir.r4.model.{CodeableConcept, Coding, DocumentManifest, DocumentReference, Extension, IdType, Patient, Reference, StringType}
import org.hl7.fhir.r4.model.Enumerations._
import org.hl7.fhir.r4.model.DocumentManifest._
import bio.ferlab.clin.etl.fhir.Model._

import scala.collection.JavaConverters._
import scala.util.Try

package object AnalysisLoader {

  def load(clinClient: IClinFhirClient, relatedAnalyses: Seq[DocumentManifest]): Seq[DocumentManifest] = ???

  def createAnalysis(clinClient: IClinFhirClient, patientId: String, status: String, analysisType:AnalysisType.Value, content:Seq[DocumentReference],
                     workflows: Seq[ClinExtension], sequencingExperiments: Seq[ClinExtension], supplementInfo: Option[String], relatedAnalyses: Option[Map[DocumentManifest, AnalysisRelationship.Value]]): DocumentManifest = {
    val documentManifest: DocumentManifest = new DocumentManifest()

    documentManifest.setId(IdType.newRandomUuid())
    documentManifest.setStatus(Try(DocumentReferenceStatus.fromCode(status)).getOrElse(DocumentReferenceStatus.NULL))
    documentManifest.setCreated(new Date())

    val typeCC: CodeableConcept = new CodeableConcept()
    val typeCoding: Coding = new Coding()
    typeCoding.setSystem("http://fhir.cqgc.ferlab.bio/CodeSystem/analysis-types")
    typeCoding.setCode(analysisType.toString)
    typeCoding.setDisplay(analysisType.display)
    typeCC.addCoding(typeCoding)
    typeCC.setText(analysisType.text)
    documentManifest.setType(typeCC)

    documentManifest.setContent(seqAsJavaList(
      content.map(doc => new Reference(doc))
    ))

    val patient: Patient = clinClient.getPatientById(new IdType(patientId))
    documentManifest.setSubject(new Reference(patient));

    documentManifest.addExtension(Utils.createExtension("http://fhir.cqgc.ferlab.bio/StructureDefinition/workflow", workflows))
    documentManifest.addExtension(Utils.createExtension("http://fhir.cqgc.ferlab.bio/StructureDefinition/sequencing-experiment", sequencingExperiments))
    if(supplementInfo.isDefined){
      val supp: Extension = new Extension()
      supp.setUrl("http://fhir.cqgc.ferlab.bio/StructureDefinition/supplementInfo")
      supp.setProperty("text", new StringType(supplementInfo.get))
    }

    if(relatedAnalyses.isDefined){
      documentManifest.setRelated(seqAsJavaList(
        relatedAnalyses.get.map{
          case (k, v) => {
            val documentManifestRelatedComponent: DocumentManifestRelatedComponent = new DocumentManifestRelatedComponent()
            documentManifestRelatedComponent.setRef((new Reference(k)).setDisplay(v.display))
          }
        }.toSeq
      ))
    }

    documentManifest
  }
}
