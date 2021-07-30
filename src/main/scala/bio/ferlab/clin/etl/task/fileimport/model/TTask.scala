package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import org.hl7.fhir.r4.model.Task.{ParameterComponent, TaskOutputComponent}
import org.hl7.fhir.r4.model.{CodeableConcept, IdType, Reference, Resource}

case class TTask() {

  def buildResource(code: String, serviceRequest: Reference, patient: Reference, owner: Reference, input: Seq[ParameterComponent], output: Seq[TaskOutputComponent], taskExtensions: TaskExtensions): Resource = {
    val t = AnalysisTask()

    t.getCode.addCoding()
      .setSystem(CodingSystems.ANALYSIS_TYPE)
      .setCode(code)

    t.setFocus(serviceRequest)
    t.setFor(patient)

    t.setOwner(owner)
    input.foreach { r =>
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
