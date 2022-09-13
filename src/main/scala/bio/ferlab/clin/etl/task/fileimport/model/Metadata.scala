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

  def validateMetadataFile(bucket: String, prefix: String, full: Boolean = false)(implicit s3Client: S3Client): ValidatedNel[String, Metadata] = {
    val objectName = s"$prefix/metadata.json"
    val get = GetObjectRequest.builder().bucket(bucket).key(objectName).build()
    Try(s3Client.getObject(get)) match {
      case Failure(e) => s"Metadata file does not exist, bucket=$bucket, prefix=$prefix, error=${e.getMessage}".invalidNel[Metadata]
      case Success(o) =>
        val bis = new ByteArrayInputStream(o.readAllBytes())

        val r = if (full) Json.parse(bis).validate[FullMetadata] else Json.parse(bis).validate[SimpleMetadata]
        r match {
          case JsSuccess(m, _) => m.validNel[String]
          case JsError(errors) =>
            val all = errors.flatMap { case (path, jsError) => jsError.map(e => s"Error parsing $path => $e") }
            NonEmptyList.fromList(all.toList).get.invalid[Metadata]
        }
    }
  }
}

sealed trait Metadata {
  def experiment: Experiment

  def workflow: Workflow

  def analyses: Seq[Analysis]
}

case class SimpleMetadata(experiment: Experiment, workflow: Workflow, analyses: Seq[SimpleAnalysis]) extends Metadata

object SimpleMetadata {
  implicit val reads: Reads[SimpleMetadata] = Json.reads[SimpleMetadata]
}


case class FullMetadata(experiment: Experiment, workflow: Workflow, analyses: Seq[FullAnalysis]) extends Metadata

object FullMetadata {
  implicit val reads: Reads[FullMetadata] = Json.reads[FullMetadata]
}


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

sealed trait Analysis {

  val ldm: String
  val ldmSampleId: String
  val ldmSpecimenId: String
  val specimenType: String
  val sampleType: Option[String]
  val labAliquotId: String
  val patient: InputPatient
  val files: FilesAnalysis
}


case class SimpleAnalysis(
                           ldm: String,
                           ldmSampleId: String,
                           ldmSpecimenId: String,
                           specimenType: String,
                           sampleType: Option[String],
                           clinServiceRequestId: String,
                           labAliquotId: String,
                           patient: SimplePatient,
                           files: FilesAnalysis
                         ) extends Analysis

object SimpleAnalysis {
  implicit val reads: Reads[SimpleAnalysis] = Json.reads[SimpleAnalysis]
}

case class FullAnalysis(

                         ldm: String,
                         ldmSampleId: String,
                         ldmSpecimenId: String,
                         specimenType: String,
                         sampleType: Option[String],
                         ldmServiceRequestId: String,
                         labAliquotId: String,
                         patient: FullPatient,
                         files: FilesAnalysis,
                         panelCode: String
                       ) extends Analysis

object FullAnalysis {
  implicit val reads: Reads[FullAnalysis] = Json.reads[FullAnalysis]
}

sealed trait InputPatient {

  def firstName: String

  def lastName: String

  def sex: String
}

case class SimplePatient(
                          clinId: String,
                          firstName: String,
                          lastName: String,
                          sex: String
                        ) extends InputPatient

object SimplePatient {
  implicit val reads: Reads[SimplePatient] = Json.reads[SimplePatient]
}

case class FullPatient(
                        firstName: String,
                        lastName: String,
                        sex: String,
                        ramq: Option[String],
                        birthDate: String,
                        mrn: Option[String],
                        ep: String,
                        designFamily: String,
                        familyMember: String,
                        familyId: Option[String],
                        status: String,
                        fetus: Option[Boolean]
                      ) extends InputPatient {
}

object FullPatient {
  implicit val reads: Reads[FullPatient] = Json.reads[FullPatient]
}

case class FilesAnalysis(cram: String, crai: String, snv_vcf: String, snv_tbi: String, cnv_vcf: String, cnv_tbi: String, supplement: String)

object FilesAnalysis {
  implicit val reads: Reads[FilesAnalysis] = Json.reads[FilesAnalysis]
}
