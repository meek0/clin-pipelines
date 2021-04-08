package bio.ferlab.clin.etl.fhir

import ca.uhn.fhir.model.api.annotation.ResourceDef
import org.hl7.fhir.r4.model.Task

@ResourceDef(name = "Task", profile = "http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-analysis-task")
class AnalysisTask extends Task {

}
