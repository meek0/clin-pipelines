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
                     specimenType: String,
                     sampleType: Option[String],
                     bodySite: String,
                     serviceRequestId: String,
                     labAliquotId: Option[String],
                     patient: InputPatient,
                     files: FilesAnalysis
                   )

object Analysis {
  implicit val reads: Reads[Analysis] = Json.reads[Analysis]
}

case class InputPatient(
                    id: String,
                    firstName: String,
                    lastName: String,
                    sex: String
                  )

object InputPatient {
  implicit val reads: Reads[InputPatient] = Json.reads[InputPatient]
}

case class FilesAnalysis(cram: String, crai: String, vcf: String, tbi: String, qc: String)

object FilesAnalysis {
  implicit val reads: Reads[FilesAnalysis] = Json.reads[FilesAnalysis]
}

