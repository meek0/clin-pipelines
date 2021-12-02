package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import org.hl7.fhir.r4.model.Task.{ParameterComponent, TaskOutputComponent}
import org.hl7.fhir.r4.model.{CodeableConcept, Extension, IdType, Reference, Resource, StringType}
import TTask._
import bio.ferlab.clin.etl.fhir.FhirUtils.ResourceExtension

case class TaskExtensions(workflowExtension: Extension, experimentExtension: Extension) {
  def forAliquot(labAliquotId: String): TaskExtensions = {
    val expExtension = experimentExtension.copy()
    expExtension.addExtension(new Extension("labAliquotId", new StringType(labAliquotId)))
    this.copy(experimentExtension = expExtension)
  }
}

case class TTask(taskExtensions: TaskExtensions) {

  def buildResource(serviceRequest: Reference, patient: Reference, organization: Reference, sample: Reference, drr: DocumentReferencesResources): Resource = {
    val t = AnalysisTask()

    t.getCode.addCoding()
      .setSystem(CodingSystems.ANALYSIS_TYPE)
      .setCode(EXOME_GERMLINE_ANALYSIS)

    t.setFocus(serviceRequest)
    t.setFor(patient)

    t.setOwner(organization)

    val input = new ParameterComponent()
    input.setType(new CodeableConcept().setText(ANALYSED_SAMPLE))
    input.setValue(sample)
    t.addInput(input)
    val sequencingExperimentOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(SequencingAlignment.documentType)
      code.setText(SequencingAlignment.label)
      val sequencingAlignment = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.sequencingAlignment.toReference())
      sequencingAlignment
    }

    val variantCallOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(VariantCalling.documentType)
      code.setText(VariantCalling.label)
      val variantCalling = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.variantCalling.toReference())
      variantCalling
    }

    val cnvOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(CopyNumberVariant.documentType)
      code.setText(CopyNumberVariant.label)
      val cnv = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.copyNumberVariant.toReference())
      cnv
    }

    val qualityControlOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(QualityControl.documentType)
      code.setText(QualityControl.label)
      val qc = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.qc.toReference())
      qc
    }

    Seq(sequencingExperimentOutput, variantCallOutput, cnvOutput, qualityControlOutput).foreach { r =>
      t.addOutput(r)
    }

    t.setId(IdType.newRandomUuid())
    t.addExtension(taskExtensions.workflowExtension)
    t.addExtension(taskExtensions.experimentExtension)
    t
  }
}

object TTask {
  val EXOME_GERMLINE_ANALYSIS = "GEAN"

  val ANALYSED_SAMPLE = "Analysed sample"
}

