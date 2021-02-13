package bio.ferlab.clin.etl.model


import org.hl7.fhir.r4.model.{Reference, Resource}

case class DocumentReferences(cram: TDocumentReference, crai: TDocumentReference, vcf: TDocumentReference, tbi: TDocumentReference, qc: TDocumentReference) {

  def buildResources(subject: Reference, custodian: Reference, sample: Reference): DocumentReferencesResources = {
    val cramR = cram.buildResource(subject, custodian, sample, None)
    val craiR = crai.buildResource(subject, custodian, sample, Some(cramR))
    val vcfR = vcf.buildResource(subject, custodian, sample, None)
    val tbiR = tbi.buildResource(subject, custodian, sample, Some(vcfR))
    val qcR = qc.buildResource(subject, custodian, sample, None)
    DocumentReferencesResources(cramR, craiR, vcfR, tbiR, qcR)
  }

}

case class DocumentReferencesResources(cram: Resource, crai: Resource, vcf: Resource, tbi: Resource, qc: Resource) {
  def resources() = Seq(cram, crai, vcf, tbi, qc)
}
