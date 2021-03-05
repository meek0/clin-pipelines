package bio.ferlab.clin.etl.model

import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model.DocumentReference.{DocumentReferenceContentComponent, DocumentReferenceContextComponent}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model._

import scala.collection.JavaConverters._

trait Document {
  val label: String
  val documentType: String
  val category: String
  val format:String
}

case object CRAI extends Document {
  override val label: String = "crai"
  override val documentType: String = "INDEX"
  override val category: String = "SR"
  override val format: String = "CRAI"
}

case object CRAM extends Document {
  override val label: String = "cram"
  override val documentType: String = "AR"
  override val category: String = "SR"
  override val format: String = "CRAM"
}

case object VCF extends Document {
  override val label: String = "vcf"
  override val documentType: String = "SNV"
  override val category: String = "SNV"
  override val format: String = "VCF"
}

case object TBI extends Document {
  override val label: String = "tbi"
  override val documentType: String = "INDEX"
  override val category: String = "INDEX"
  override val format: String = "TBI"
}

case object QC extends Document {
  override val documentType: String = "QC"
  override val label: String = "qc"
  override val category: String = "SR"
  override val format: String = "TGZ"
}

case class TDocumentReference(objectStoreId: String, title: String, md5: String, size: Long, document: Document) {

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
      .setCode(document.documentType)
    dr.addCategory().addCoding()
      .setSystem(CodingSystems.DR_CATEGORY)
      .setCode(document.category)
    val a = new Attachment()
    a.setContentType("application/binary")
    a.setUrl(s"https://objectstore.cqgc.ca/$objectStoreId")
    a.setHash(md5.getBytes())
    a.setTitle(title)
    a.setSizeElement(new UnsignedIntType(size))

    val drcc = new DocumentReferenceContentComponent(a)
    drcc.getFormat.setSystem(CodingSystems.DR_FORMAT).setCode(document.format)
    dr.setContent(List(drcc).asJava)
  }
}
