package bio.ferlab.clin.etl.fhir

import ca.uhn.fhir.model.api.annotation.ResourceDef
import org.hl7.fhir.r4.model.{DateTimeType, Task}
import org.hl7.fhir.r4.model.Task.{TaskIntent, TaskPriority, TaskStatus}

import java.util.Date

@ResourceDef(name = "Task", profile = "http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-analysis-task")
class AnalysisTask extends Task {

}

object AnalysisTask{
  def apply(): AnalysisTask = {
    val t = new AnalysisTask()
    t.setStatus(TaskStatus.COMPLETED)
    t.setIntent(TaskIntent.ORDER)
    t.setPriority(TaskPriority.ROUTINE)
    t.setAuthoredOnElement(new DateTimeType(new Date()))
    t
  }
}