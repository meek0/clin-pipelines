package bio.ferlab.clin.etl.model

import bio.ferlab.clin.etl.model.Fhir.FhirResource
import org.hl7.fhir.r4.model.{IdType, Reference}

case class DocumentReferences(cram: DocumentReference, crai: DocumentReference, vcf: DocumentReference, tbi: DocumentReference, qc: DocumentReference) {

  def buildResources(subject: Reference, custodian: Reference, sample: Reference) = {
    val cramR = cram.buildResource(subject, custodian, sample, None)
    val craiR = crai.buildResource(subject, custodian, sample, Some(cramR))
    val vcfR = vcf.buildResource(subject, custodian, sample, None)
    val tbiR = tbi.buildResource(subject, custodian, sample, Some(vcfR))
    val qcR = qc.buildResource(subject, custodian, sample, None)
    DocumentReferencesResources(cramR, craiR, vcfR, tbiR, qcR)
  }

}

case class DocumentReferencesResources(cram: FhirResource, crai: FhirResource, vcf: FhirResource, tbi: FhirResource, qc: FhirResource) {
  def resources() = Seq(cram, crai, vcf, tbi, qc)
}
