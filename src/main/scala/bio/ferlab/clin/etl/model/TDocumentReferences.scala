package bio.ferlab.clin.etl.model

import org.hl7.fhir.r4.model.{Reference, Resource}

case class TDocumentReferences(sequencingAlignment: TDocumentReference, variantCalling: TDocumentReference, qc: TDocumentReference) {

  def buildResources(subject: Reference, custodian: Reference, sample: Reference): DocumentReferencesResources = {
    val sequencingAlignmentR = sequencingAlignment.buildResource(subject, custodian, sample, None)
    val variantCallingR = variantCalling.buildResource(subject, custodian, sample, None)
    val qcR = qc.buildResource(subject, custodian, sample, None)
    DocumentReferencesResources(sequencingAlignmentR, variantCallingR, qcR)
  }

}

case class DocumentReferencesResources(sequencingAlignment: Resource, variantCalling: Resource, qc: Resource) {
  def resources() = Seq(sequencingAlignment, variantCalling, qc)
}
