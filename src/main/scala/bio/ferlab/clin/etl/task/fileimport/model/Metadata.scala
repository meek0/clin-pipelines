package bio.ferlab.clin.etl.task.fileimport.model

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits.catsSyntaxValidatedId
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json, Reads}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.io.ByteArrayInputStream
import scala.io.Source
import scala.util.{Failure, Success, Try}

object Metadata {
  implicit val reads: Reads[Metadata] = Json.reads[Metadata]

  def validateMetadataFile(bucket: String, prefix: String)(implicit s3Client: S3Client): ValidatedNel[String, Metadata] = {
    val objectName = s"$prefix/metadata.json"
    val get = GetObjectRequest.builder().bucket(bucket).key(objectName).build()
    Try(s3Client.getObject(get)) match {
      case Failure(e) => s"Error duriung fetching metadata file does not exist, bucket=$bucket, prefix=$prefix, error=${e.getMessage}".invalidNel[Metadata]
      case Success(o) =>
        val bis = new ByteArrayInputStream(o.readAllBytes())

        val r = Json.parse(bis).validate[Metadata]
        r match {
          case JsSuccess(m, _) => m.validNel[String]
          case JsError(errors) =>
            val all = errors.flatMap { case (path, jsError) => jsError.map(e => s"Error parsing $path => $e") }
            NonEmptyList.fromList(all.toList).get.invalid[Metadata]
        }
    }
  }
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
                     ldmSampleId: String,
                     ldmSpecimenId: String,
                     specimenType: String,
                     sampleType: Option[String],
                     bodySite: String,
                     clinServiceRequestId: String,
                     labAliquotId: String,
                     patient: InputPatient,
                     files: FilesAnalysis
                   )

object Analysis {
  implicit val reads: Reads[Analysis] = Json.reads[Analysis]
}

case class InputPatient(
                         clinId: String,
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

