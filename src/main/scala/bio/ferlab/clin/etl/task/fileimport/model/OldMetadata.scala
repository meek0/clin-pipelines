package bio.ferlab.clin.etl.task.fileimport.model

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits.catsSyntaxValidatedId
import play.api.libs.json.{JsError, JsSuccess, Json, Reads}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.io.ByteArrayInputStream
import scala.util.{Failure, Success, Try}

object OldMetadata {
  implicit val reads: Reads[OldMetadata] = Json.reads[OldMetadata]

  def validateMetadataFile(bucket: String, prefix: String)(implicit s3Client: S3Client): ValidatedNel[String, OldMetadata] = {
    val objectName = s"$prefix/metadata.json"
    val get = GetObjectRequest.builder().bucket(bucket).key(objectName).build()
    Try(s3Client.getObject(get)) match {
      case Failure(e) => s"Metadata file does not exist, bucket=$bucket, prefix=$prefix, error=${e.getMessage}".invalidNel[OldMetadata]
      case Success(o) =>
        val bis = new ByteArrayInputStream(o.readAllBytes())

        val r = Json.parse(bis).validate[OldMetadata]
        r match {
          case JsSuccess(m, _) => m.validNel[String]
          case JsError(errors) =>
            val all = errors.flatMap { case (path, jsError) => jsError.map(e => s"Error parsing $path => $e") }
            NonEmptyList.fromList(all.toList).get.invalid[OldMetadata]
        }
    }
  }

}

case class OldMetadata(submissionSchema: Option[String], experiment: Experiment, workflow: Workflow, analyses: Seq[OldAnalysis]) {
  def toNewMetadata: FullMetadata = {
    val newAnalyses = analyses.map { a =>
      FullAnalysis(
        a.ldm,
        a.ldmSampleId,
        a.ldmSpecimenId,
        a.specimenType,
        a.sampleType,
        a.ldmServiceRequestId,
        a.labAliquotId,
        a.priority,
        a.patient,
        a.files,
        Some(a.panelCode),
        None,
        experiment,
        workflow
      )
    }
    FullMetadata(this.submissionSchema, newAnalyses)
  }
}

case class OldAnalysis(

                        ldm: String,
                        ldmSampleId: String,
                        ldmSpecimenId: String,
                        specimenType: String,
                        sampleType: Option[String],
                        ldmServiceRequestId: String,
                        labAliquotId: String,
                        patient: FullPatient,
                        priority: Option[AnalysisPriority.Value],
                        files: FilesAnalysis,
                        panelCode: String
                      )

object OldAnalysis {
  implicit val reads: Reads[OldAnalysis] = Json.reads[OldAnalysis]
}
