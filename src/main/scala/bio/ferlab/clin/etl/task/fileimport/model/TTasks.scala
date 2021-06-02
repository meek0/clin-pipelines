package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.{CodingSystems, Extensions}
import bio.ferlab.clin.etl.fhir.FhirUtils.ResourceExtension
import bio.ferlab.clin.etl.task.fileimport.model.TTasks._
import org.hl7.fhir.r4.model.Task.{ParameterComponent, TaskOutputComponent}
import org.hl7.fhir.r4.model._

case class TaskExtensions(workflowExtension: Extension, experimentExtension: Extension)

case class TTasks(taskExtensions: TaskExtensions) {
  val sequencingAlignment: TTask = TTask()
  val variantCall: TTask = TTask()
  val qualityControl: TTask = TTask()


  def buildResources(serviceRequest: Reference, patient: Reference, organization: Reference, sample: Reference, drr: DocumentReferencesResources): Seq[Resource] = {

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
      sequencingAlignment.buildResource(SEQUENCING_ALIGNMENT_ANALYSIS, serviceRequest, patient, organization, sequencingExperimentInput, sequencingExperimentOutput, taskExtensions)
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
    val variantCallR = variantCall.buildResource(VARIANT_CALLING_ANALYSIS, serviceRequest, patient, organization, variantCallInput, variantCallOutput, taskExtensions)

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
    val qualityControlR = qualityControl.buildResource(SEQUENCING_QC_ANALYSIS, serviceRequest, patient, organization, qualityControlInput, qualityControlOutput, taskExtensions)

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
