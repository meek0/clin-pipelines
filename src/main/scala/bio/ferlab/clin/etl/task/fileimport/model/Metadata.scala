package bio.ferlab.clin.etl.task.fileimport.model

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits.catsSyntaxValidatedId
import play.api.libs.json.{JsError, JsSuccess, Json, Reads, Writes}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.io.ByteArrayInputStream
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

  def analyses: Seq[Analysis]
}

case class SimpleMetadata(submissionSchema: Option[String], analyses: Seq[SimpleAnalysis]) extends Metadata

object SimpleMetadata {
  implicit val reads: Reads[SimpleMetadata] = Json.reads[SimpleMetadata]
}


case class FullMetadata(submissionSchema: Option[String], analyses: Seq[FullAnalysis]) extends Metadata

object FullMetadata {
  implicit val reads: Reads[FullMetadata] = Json.reads[FullMetadata]
  implicit val writes: Writes[FullMetadata] = Json.writes[FullMetadata]
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
  implicit val writes: Writes[Experiment] = Json.writes[Experiment]
}

case class Workflow(
                     name: Option[String],
                     version: Option[String],
                     genomeBuild: Option[String],
                   )

object Workflow {
  implicit val reads: Reads[Workflow] = Json.reads[Workflow]
  implicit val writes: Writes[Workflow] = Json.writes[Workflow]
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

  def experiment: Experiment

  def workflow: Workflow
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
                           files: FilesAnalysis,
                           experiment: Experiment,
                           workflow: Workflow
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
                         panelCode: String,
                         experiment: Experiment,
                         workflow: Workflow
                       ) extends Analysis

object FullAnalysis {
  implicit val reads: Reads[FullAnalysis] = Json.reads[FullAnalysis]
  implicit val writes: Writes[FullAnalysis] = Json.writes[FullAnalysis]
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
  implicit val writes: Writes[FullPatient] = Json.writes[FullPatient]
}

case class FilesAnalysis(cram: String, crai: String, snv_vcf: String, snv_tbi: String, cnv_vcf: String, cnv_tbi: String, sv_vcf: Option[String], sv_tbi: Option[String], supplement: String,
                         exomiser_html: Option[String], exomiser_json: Option[String], exomiser_variants_tsv: Option[String], seg_bw: Option[String], hard_filtered_baf_bw: Option[String],
                         roh_bed: Option[String], hyper_exome_hg38_bed: Option[String], cnv_calls_png: Option[String], coverage_by_gene_csv: Option[String], qc_metrics: Option[String], qc_metrics_tsv: Option[String])

object FilesAnalysis {
  implicit val reads: Reads[FilesAnalysis] = Json.reads[FilesAnalysis]
  implicit val writes: Writes[FilesAnalysis] = Json.writes[FilesAnalysis]
}
