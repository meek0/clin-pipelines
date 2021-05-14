package bio.ferlab.clin.etl.model

import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model.DocumentReference.{DocumentReferenceContentComponent, DocumentReferenceContextComponent}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model._

import scala.collection.JavaConverters._

case class TDocumentReference(document: Seq[TDocumentAttachment], documentReferenceType: DocumentReferenceType) {

  def validateBaseResource()(implicit fhirClient: IGenericClient): OperationOutcome = {
    val baseResource = buildBase()
    FhirUtils.validateResource(baseResource)
  }

  def buildResource(subject: Reference, custodian: Reference, sample: Reference, related: Option[Reference]): Resource = {
    val dr = buildBase()
    val drc = new DocumentReferenceContextComponent()
    related.foreach(r => drc.setRelated(List(r).asJava))

    dr.setContext(drc)

    dr.setId(IdType.newRandomUuid())
    dr.setSubject(subject)
    dr.setCustodian(custodian)
    dr

  }

  private def buildBase() = {
    val dr = new DocumentReference()
    dr.setStatus(DocumentReferenceStatus.CURRENT)
    dr.getType.addCoding()
      .setSystem(CodingSystems.DR_TYPE)
      .setCode(documentReferenceType.documentType)
    dr.addCategory().addCoding()
      .setSystem(CodingSystems.DR_CATEGORY)
      .setCode(documentReferenceType.category)
    val components = document.map{ d=>
      val a = new Attachment()
      a.setContentType("application/binary")
      a.setUrl(s"https://objectstore.cqgc.ca/${d.objectStoreId}")
      a.setHash(d.md5.getBytes())
      a.setTitle(d.title)
      a.setSizeElement(new UnsignedIntType(d.size))
      val drcc = new DocumentReferenceContentComponent(a)
      drcc.getFormat.setSystem(CodingSystems.DR_FORMAT).setCode(d.format)
      drcc
    }

    dr.setContent(components.asJava)
  }
}

trait DocumentReferenceType {
  val documentType: String
  val category: String
}

case object SequencingAlignment extends DocumentReferenceType {
  override val documentType: String = "AR"
  override val category: String = "SR"
}

case object VariantCalling extends DocumentReferenceType {
  override val documentType: String = "SNV"
  override val category: String = "SNV"
}

case object QualityControl extends DocumentReferenceType {
  override val documentType: String = "QC"
  override val category: String = "RE"
}

trait TDocumentAttachment {
  val format: String
  val objectStoreId: String
  val title: String
  val md5: String
  val size: Long
}

case class CRAI(objectStoreId: String, title: String, md5: String, size: Long) extends TDocumentAttachment {
  override val format: String = "CRAI"
}

case class CRAM(objectStoreId: String, title: String, md5: String, size: Long) extends TDocumentAttachment {
  override val format: String = "CRAM"
}

case class VCF(objectStoreId: String, title: String, md5: String, size: Long) extends TDocumentAttachment {
  override val format: String = "VCF"
}

case class TBI(objectStoreId: String, title: String, md5: String, size: Long) extends TDocumentAttachment {
  override val format: String = "TBI"
}

case class QC(objectStoreId: String, title: String, md5: String, size: Long) extends TDocumentAttachment {
  override val format: String = "TGZ"
}
