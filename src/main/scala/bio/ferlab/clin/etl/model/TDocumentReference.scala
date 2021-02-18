package bio.ferlab.clin.etl.model

import bio.ferlab.clin.etl.fhir.FhirUtils
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.hl7.fhir.r4.model.DocumentReference.{DocumentReferenceContentComponent, DocumentReferenceContextComponent}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model.{Attachment, DocumentReference, IdType, OperationOutcome, Reference, Resource, UnsignedIntType}

import java.nio.charset.Charset
import java.util.Base64
import scala.collection.JavaConverters._

case class TDocumentReference(objectStoreId: String, title: String, md5: String, size: Long) {

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

    val a = new Attachment()
    a.setContentType("application/binary")
    a.setUrl(s"https://objectstore.cqgc.ca/$objectStoreId")
    a.setHash(md5.getBytes())
    a.setTitle(title)
    a.setSizeElement(new UnsignedIntType(size))
    val drcc = new DocumentReferenceContentComponent(a)
    dr.setContent(List(drcc).asJava)
  }
}
