package bio.ferlab.clin.etl.fhir

import java.nio.charset.StandardCharsets
import java.util.{Collections, Date}

import bio.ferlab.clin.etl.fhir.Model.{ClinExtension, FileInfo, Terminology}
import org.hl7.fhir.r4.model.DocumentReference._
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model._

import scala.collection.JavaConverters._
import scala.util.Try

package object FileLoader {

  def load(clinClient: IClinFhirClient): Seq[DocumentReference] = ???

  def createFile(clinClient: IClinFhirClient, objectStoreAddress: String, objectStoreFileId: String, status: String, docStatus: String,
                 fileType: Terminology, fileCategory: Terminology, patientId: String, custodianOrganizationId: String, fileFormat: Terminology,
                 fileInfo: FileInfo, specimens: Option[Seq[Specimen]], alignmentMetrics: Option[Seq[ClinExtension]],
                 relatedDocuments: Option[Map[DocumentReference, String]]): DocumentReference = {

    val file:DocumentReference = new DocumentReference()

    file.setId(IdType.newRandomUuid())
    file.setDate(new Date())

    val masterIdentifier: Identifier = new Identifier()
    masterIdentifier.setSystem(objectStoreAddress)
    masterIdentifier.setValue(objectStoreFileId)
    file.setMasterIdentifier(masterIdentifier)

    file.setStatus(Try(DocumentReferenceStatus.fromCode(status)).getOrElse(DocumentReferenceStatus.NULL))
    file.setDocStatus(Try(ReferredDocumentStatus.fromCode(docStatus)).getOrElse(ReferredDocumentStatus.NULL))

    val fileTypeCC: CodeableConcept = new CodeableConcept()
    val fileTypeCoding: Coding = new Coding()
    fileTypeCoding.setSystem(fileType.system)
    fileTypeCoding.setCode(fileType.code)
    fileTypeCoding.setDisplay(fileType.display)
    fileTypeCC.addCoding(fileTypeCoding)
    file.setType(fileTypeCC)

    val categoryCC: CodeableConcept = new CodeableConcept()
    val categoryCoding: Coding = new Coding()
    categoryCoding.setSystem(fileCategory.system)
    categoryCoding.setCode(fileCategory.code)
    categoryCoding.setDisplay(fileCategory.display)
    categoryCC.addCoding(categoryCoding)
    file.setCategory(Collections.singletonList(categoryCC))

    val patient: Patient = clinClient.getPatientById(new IdType(patientId))
    file.setSubject(new Reference(patient))

    val org: Organization = clinClient.getOrganizationById(new IdType(custodianOrganizationId));
    file.setCustodian(new Reference(org))

    val securityCC: CodeableConcept = new CodeableConcept()
    val securityCoding: Coding = new Coding()
    securityCoding.setSystem("http://terminology.hl7.org/CodeSystem/v3-Confidentiality")
    securityCoding.setCode("V")
    securityCoding.setDisplay("very restricted")
    securityCC.addCoding(securityCoding)
    file.setSecurityLabel(Collections.singletonList(securityCC))

    val content:DocumentReferenceContentComponent = new DocumentReferenceContentComponent()
    val attachment:Attachment = new Attachment()
    attachment.setContentType(fileInfo.contentType)
    attachment.setUrl(fileInfo.url)
    attachment.setSize(fileInfo.size)
    attachment.setHash(fileInfo.hash.getBytes(StandardCharsets.UTF_8))
    attachment.setTitle(fileInfo.title)
    attachment.setCreation(fileInfo.creationDate)

    val format:Coding = new Coding()
    format.setSystem(fileFormat.system)
    format.setCode(fileFormat.code)
    format.setDisplay(fileFormat.display)

    content.setFormat(format)
    content.setAttachment(attachment)
    file.setContent(Collections.singletonList(content))

    if(specimens.isDefined){
      val docContext: DocumentReferenceContextComponent = new DocumentReferenceContextComponent()
      val references: Seq[Reference] = specimens.get.map(specimen => (new Reference(specimen)).setDisplay("Aliquot"))
      docContext.setRelated(seqAsJavaList(references))
      file.setContext(docContext)
    }

    // CRAI relates to CRAM
    // TBI  relates to VCF
    if(relatedDocuments.isDefined){
      val relatesTo: Seq[DocumentReferenceRelatesToComponent] = relatedDocuments.get.map{
        case (relatedDoc, docType) => {
          val doc: DocumentReferenceRelatesToComponent = new DocumentReferenceRelatesToComponent()
          doc.setCode(Try(DocumentRelationshipType.fromCode(docType)).getOrElse(DocumentRelationshipType.NULL))
          doc.setTarget(new Reference(relatedDoc))
        }
      }.toSeq

      file.setRelatesTo(seqAsJavaList(relatesTo))
    }

    if(alignmentMetrics.isDefined){
      file.addExtension(Utils.createExtension("http://fhir.cqgc.ferlab.bio/StructureDefinition/sequencing-metrics", alignmentMetrics.get))
    }
    file
  }
}
