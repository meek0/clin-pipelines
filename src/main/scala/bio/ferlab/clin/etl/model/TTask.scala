package bio.ferlab.clin.etl.model

import bio.ferlab.clin.etl.fhir.AnalysisTask
import org.hl7.fhir.r4.model.Task.{ParameterComponent, TaskIntent, TaskOutputComponent, TaskPriority, TaskStatus}
import org.hl7.fhir.r4.model.{CodeableConcept, DateTimeType, Extension, IdType, Reference, Resource, Task}

import java.util.Date

case class TTask() {

  def buildResource(code: String, serviceRequest: Reference, patient: Reference, owner: Reference, input: Seq[ParameterComponent], output: Seq[TaskOutputComponent], workflow: Extension, experiment: Extension): Resource = {
    val t = new AnalysisTask()
    t.addBasedOn()
    t.setStatus(TaskStatus.COMPLETED)
    t.setIntent(TaskIntent.ORDER)
    t.setPriority(TaskPriority.ROUTINE)
    t.setPriority(TaskPriority.ROUTINE)
    t.setAuthoredOnElement(new DateTimeType(new Date()))
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
    t.addExtension(workflow)
    t.addExtension(experiment)
    t
  }
}
