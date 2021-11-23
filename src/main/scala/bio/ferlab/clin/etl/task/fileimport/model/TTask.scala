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
    //    labAliquotId.foreach(v => expExtension.addExtension(new Extension("labAliquotId", new StringType(v))))
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
      val sequencingAlignment = new TaskOutputComponent()
        .setType(new CodeableConcept().setText(CRAM_FILE)) //TODO Use a terminology
        .setValue(drr.sequencingAlignment.toReference())
      sequencingAlignment
    }

    val variantCallOutput = {
      val variantCalling = new TaskOutputComponent()
        .setType(new CodeableConcept().setText(VCF_FILE)) //TODO Use a terminology
        .setValue(drr.variantCalling.toReference())
      variantCalling
    }

    val qualityControlOutput = {
      val qc = new TaskOutputComponent()
        .setType(new CodeableConcept().setText(QC_FILE)) //TODO Use a terminology
        .setValue(drr.qc.toReference())
      qc
    }

    Seq(sequencingExperimentOutput, variantCallOutput, qualityControlOutput).foreach { r =>
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

  val CRAM_FILE = "CRAM File"

  val CRAI_FILE = "CRAI File"

  val VCF_FILE = "VCF File"

  val TBI_FILE = "TBI File"

  val QC_FILE = "QC File"
}
