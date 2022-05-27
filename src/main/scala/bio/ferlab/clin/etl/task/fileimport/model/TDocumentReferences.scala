package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.conf.FerloadConf
import org.hl7.fhir.r4.model.{Reference, Resource}

case class TDocumentReferences(sequencingAlignment: SequencingAlignment, variantCalling: VariantCalling, copyNumberVariant: CopyNumberVariant, supplement: SupplementDocument) {

  def buildResources(subject: Reference, custodian: Reference, sample: Reference)(implicit ferloadConf: FerloadConf): DocumentReferencesResources = {
    val sequencingAlignmentR = sequencingAlignment.buildResource(subject, custodian, Seq(sample))
    val variantCallingR = variantCalling.buildResource(subject, custodian, Seq(sample))
    val copyNumberVariantR = copyNumberVariant.buildResource(subject, custodian, Seq(sample))
    val supplementR = supplement.buildResource(subject, custodian, Seq(sample))
    DocumentReferencesResources(sequencingAlignmentR, variantCallingR, copyNumberVariantR, supplementR)
  }

}

case class DocumentReferencesResources(sequencingAlignment: Resource, variantCalling: Resource, copyNumberVariant:Resource, supplement: Resource) {
  def resources() = Seq(sequencingAlignment, variantCalling, copyNumberVariant, supplement)
}
