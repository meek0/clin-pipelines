package bio.ferlab.clin.etl.task.fileimport.model

import org.hl7.fhir.r4.model.Task.{ParameterComponent, TaskOutputComponent}
import org.hl7.fhir.r4.model.{CodeableConcept, IdType, Reference, Resource}

case class TTask() {

  def buildResource(code: String, serviceRequest: Reference, patient: Reference, owner: Reference, input: Seq[ParameterComponent], output: Seq[TaskOutputComponent], taskExtensions: TaskExtensions): Resource = {
    val t = AnalysisTask()
    t.setCode(new CodeableConcept().setText(code)) //TODO Use a terminology
    t.setFocus(serviceRequest)
    t.setFor(patient)

    t.setOwner(owner)
    input.foreach { r =>
      //      val p = new ParameterComponent()
      //      p.setType(new CodeableConcept().setText("Analysed sample"))
      //      p.setValue(r)
      t.addInput(r)
    }
    output.foreach { r =>
      t.addOutput(r)
    }


    t.setId(IdType.newRandomUuid())
    t.addExtension(taskExtensions.workflowExtension)
    t.addExtension(taskExtensions.experimentExtension)
    t
  }
}
