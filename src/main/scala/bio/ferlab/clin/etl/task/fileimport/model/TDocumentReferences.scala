package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.conf.FerloadConf
import org.hl7.fhir.r4.model.{Reference, Resource}

case class TDocumentReferences(sequencingAlignment: SequencingAlignment, variantCalling: VariantCalling, copyNumberVariant: CopyNumberVariant, structuralVariant: StructuralVariant, supplement: SupplementDocument,
                               exomiser: Exomiser, igvTrack: IgvTrack, cnvVisualization: CnvVisualization, coverageByGene: CoverageByGene, qcMetrics: QcMetrics) {

  def buildResources(subject: Reference, custodian: Reference, sample: Reference)(implicit ferloadConf: FerloadConf): DocumentReferencesResources = {
    val sequencingAlignmentR = sequencingAlignment.buildResource(subject, custodian, Seq(sample))
    val variantCallingR = variantCalling.buildResource(subject, custodian, Seq(sample))
    val copyNumberVariantR = copyNumberVariant.buildResource(subject, custodian, Seq(sample))
    val structuralVariantR = structuralVariant.buildResource(subject, custodian, Seq(sample))
    val supplementR = supplement.buildResource(subject, custodian, Seq(sample))
    val exomiserR = exomiser.buildResource(subject, custodian, Seq(sample))
    val igvTrackR = igvTrack.buildResource(subject, custodian, Seq(sample))
    val cnvVisualizationR = cnvVisualization.buildResource(subject, custodian, Seq(sample))
    val coverageByGeneR = coverageByGene.buildResource(subject, custodian, Seq(sample))
    val qcMetricsR = qcMetrics.buildResource(subject, custodian, Seq(sample))
    DocumentReferencesResources(sequencingAlignmentR, variantCallingR, copyNumberVariantR, structuralVariantR, supplementR,
      exomiserR, igvTrackR, cnvVisualizationR, coverageByGeneR, qcMetricsR)
  }

}

case class DocumentReferencesResources(sequencingAlignment: Resource, variantCalling: Resource, copyNumberVariant:Resource, structuralVariant:Resource, supplement: Resource,
                                       exomiser: Resource, igvTrack: Resource, cnvVisualization: Resource, coverageByGene: Resource, qcMetrics: Resource) {
  def resources() = Seq(sequencingAlignment, variantCalling, copyNumberVariant, structuralVariant, supplement, exomiser, igvTrack, cnvVisualization, coverageByGene, qcMetrics)
}
