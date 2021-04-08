package bio.ferlab.clin.etl.model

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.{CodingSystems, Extensions}
import bio.ferlab.clin.etl.fhir.FhirUtils.ResourceExtension
import bio.ferlab.clin.etl.model.TTasks._
import org.hl7.fhir.r4.model.Task.{ParameterComponent, TaskOutputComponent}
import org.hl7.fhir.r4.model._

case class TTasks( workflow: Workflow, experiment: Experiment) {
  val sequencingAlignment: TTask = TTask()
  val variantCall: TTask = TTask()
  val qualityControl: TTask = TTask()

  private def buildWorkflowExtension() = {
    val workflowExtension = new Extension(Extensions.WORKFLOW);
    workflow.name.foreach { name => workflowExtension.addExtension(new Extension("workflowName", new StringType(name))) }
    workflow.genomeBuild.foreach { genomeBuild =>
      val code = new Coding()
      code.setCode(genomeBuild).setSystem(CodingSystems.GENOME_BUILD)
      workflowExtension.addExtension(new Extension("genomeBuild", code))
    }
    workflow.version.foreach { version => workflowExtension.addExtension(new Extension("workflowVersion", new StringType(version))) }
    workflowExtension
  }

  private def buildExperimentExtension() = {
    val expExtension = new Extension(Extensions.SEQUENCING_EXPERIMENT)
    experiment.runName.foreach { v => expExtension.addExtension(new Extension("runName", new StringType(v))) }
    experiment.runDate.foreach { v => expExtension.addExtension(new Extension("runDate", new DateTimeType(v))) }
    experiment.runAlias.foreach { v => expExtension.addExtension(new Extension("runAlias", new StringType(v))) }
    experiment.experimentalStrategy.foreach { v =>
      val code = new Coding()
      code.setCode(v).setSystem(CodingSystems.EXPERIMENTAL_STRATEGY)
      expExtension.addExtension(new Extension("experimentalStrategy", code))
    }
    experiment.platform.foreach { v => expExtension.addExtension(new Extension("platform", new StringType(v))) }
    experiment.captureKit.foreach { v => expExtension.addExtension(new Extension("captureKit", new StringType(v))) }
    experiment.sequencerId.foreach { v => expExtension.addExtension(new Extension("sequencerId", new StringType(v))) }
    expExtension
  }


  def buildResources(serviceRequest: Reference, patient: Reference, organization: Reference, sample: Reference, drr: DocumentReferencesResources): Seq[Resource] = {

    val workflowExt = buildWorkflowExtension()
    val experiementExt = buildExperimentExtension()
    val sequencingExperimentInput = {
      val p = new ParameterComponent()
      p.setType(new CodeableConcept().setText(ANALYSED_SAMPLE))
      Seq(p.setValue(sample))
    }
    val sequencingExperimentOutput = {
      val sequencingAlignment = new TaskOutputComponent()
        .setType(new CodeableConcept().setText(CRAM_FILE)) //TODO Use a terminology
        .setValue(drr.sequencingAlignment.toReference())
      Seq(sequencingAlignment)
    }
    val sequencingAlignmentR =
      sequencingAlignment.buildResource(SEQUENCING_ALIGNMENT_ANALYSIS, serviceRequest, patient, organization, sequencingExperimentInput, sequencingExperimentOutput, workflowExt, experiementExt)

    val variantCallInput = {
      val p = new ParameterComponent()
        .setType(new CodeableConcept().setText(SEQUENCING_ALIGNMENT_ANALYSIS)) //TODO Use a terminology
      Seq(p.setValue(sequencingAlignmentR.toReference()))
    }
    val variantCallOutput = {
      val variantCalling = new TaskOutputComponent()
        .setType(new CodeableConcept().setText(VCF_FILE)) //TODO Use a terminology
        .setValue(drr.variantCalling.toReference())
      Seq(variantCalling)
    }
    val variantCallR = variantCall.buildResource(VARIANT_CALLING_ANALYSIS, serviceRequest, patient, organization, variantCallInput, variantCallOutput, workflowExt, experiementExt)

    val qualityControlInput = {
      val sq = new ParameterComponent()
        .setType(new CodeableConcept().setText(SEQUENCING_ALIGNMENT_ANALYSIS)) //TODO Use a terminology
        .setValue(sequencingAlignmentR.toReference())
      val variantCall = new ParameterComponent()
        .setType(new CodeableConcept().setText(VARIANT_CALLING_ANALYSIS)) //TODO Use a terminology
        .setValue(variantCallR.toReference())
      Seq(sq, variantCall)
    }

    val qualityControlOutput = {
      val qc = new TaskOutputComponent()
        .setType(new CodeableConcept().setText(QC_FILE)) //TODO Use a terminology
        .setValue(drr.qc.toReference())
      Seq(qc)
    }
    val qualityControlR = qualityControl.buildResource(SEQUENCING_QC_ANALYSIS, serviceRequest, patient, organization, qualityControlInput, qualityControlOutput, workflowExt, experiementExt)

    Seq(sequencingAlignmentR, variantCallR, qualityControlR)

  }

}

object TTasks {
  val SEQUENCING_ALIGNMENT_ANALYSIS = "Sequencing Alignment Analysis"

  val ANALYSED_SAMPLE = "Analysed sample"

  val CRAM_FILE = "CRAM File"

  val CRAI_FILE = "CRAI File"

  val VCF_FILE = "VCF File"

  val TBI_FILE = "TBI File"

  val VARIANT_CALLING_ANALYSIS = "Variant Calling Analysis"

  val SEQUENCING_QC_ANALYSIS = "Sequencing QC Analysis"

  val QC_FILE = "QC File"

  val allTypes = Seq(SEQUENCING_ALIGNMENT_ANALYSIS, VARIANT_CALLING_ANALYSIS, SEQUENCING_QC_ANALYSIS)
}
