package bio.ferlab.clin.etl.model

import bio.ferlab.clin.etl.task.FerloadConf
import org.hl7.fhir.r4.model.{Reference, Resource}

case class TDocumentReferences(sequencingAlignment: SequencingAlignment, variantCalling: VariantCalling, qc: QualityControl) {

  def buildResources(subject: Reference, custodian: Reference, sample: Reference)(implicit ferloadConf: FerloadConf): DocumentReferencesResources = {
    val sequencingAlignmentR = sequencingAlignment.buildResource(subject, custodian, Seq(sample))
    val variantCallingR = variantCalling.buildResource(subject, custodian, Seq(sample))
    val qcR = qc.buildResource(subject, custodian, Seq(sample))
    DocumentReferencesResources(sequencingAlignmentR, variantCallingR, qcR)
  }

}

case class DocumentReferencesResources(sequencingAlignment: Resource, variantCalling: Resource, qc: Resource) {
  def resources() = Seq(sequencingAlignment, variantCalling, qc)
}
