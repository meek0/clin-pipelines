package bio.ferlab.clin.etl.model

import play.api.libs.json.{Json, Reads}

object Metadata {
  implicit val reads: Reads[Metadata] = Json.reads[Metadata]
}

case class Metadata(experiment: Experiment, workflow: Workflow, analyses: Seq[Analysis])

case class Experiment(
                       platform: Option[String],
                       sequencerId: Option[String],
                       runName: Option[String],
                       runDate: Option[String],
                       runAlias: Option[String],
                       flowcellId: Option[String],
                       isPairedEnd: Option[Boolean],
                       fragmentSize: Option[Int],
                       experimentalStrategy: Option[String],
                       captureKit: Option[String],
                       baitDefinition: Option[String]
                     )

object Experiment {
  implicit val reads: Reads[Experiment] = Json.reads[Experiment]
}

case class Workflow(
                     name: Option[String],
                     version: Option[String],
                     genomeBuild: Option[String],
                   )

object Workflow {
  implicit val reads: Reads[Workflow] = Json.reads[Workflow]
}

case class Analysis(

                     ldm: String,
                     sampleId: String,
                     specimenId: String,
                     specimenType: Option[String],
                     sampleType: Option[String],
                     bodySite: Option[String],
                     serviceRequestId: String,
                     labAliquotId: Option[String],
                     patient: Patient,
                     files: FileAnalyses
                   )

object Analysis {
  implicit val reads: Reads[Analysis] = Json.reads[Analysis]
}

case class Patient(
                    id: String,
                    firstName: String,
                    lastName: String,
                    sex: String
                  )

object Patient {
  implicit val reads: Reads[Patient] = Json.reads[Patient]
}

case class FileAnalyses(SA: Seq[FileAnalysis], VC: Seq[FileAnalysis], QC: Seq[FileAnalysis])

object FileAnalyses {
  implicit val reads: Reads[FileAnalyses] = Json.reads[FileAnalyses]
}

case class FileAnalysis(
                         title: String,
                         fileType: String
                       )
object FileAnalysis {
  implicit val reads: Reads[FileAnalysis] = Json.reads[FileAnalysis]
}
