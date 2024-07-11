package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.{LOGGER, SomaticNormalImport, ValidationResult}
import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data.attach
import bio.ferlab.clin.etl.task.fileimport.model.TTask.{EXOME_GERMLINE_ANALYSIS, EXTUM_ANALYSIS, SOMATIC_NORMAL}
import bio.ferlab.clin.etl.task.fileimport.model.{Analysis, FileEntry, FullMetadata, IgvTrack, RawFileEntry}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import cats.implicits.catsSyntaxValidatedId
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsSuccess, Json}
import software.amazon.awssdk.services.s3.S3Client
import cats.data.Validated.{Invalid, Valid}
import org.apache.commons.lang3.StringUtils
import org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.{Bundle, Task}

import java.util.UUID
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case object FixBatchFiles {

  val LOGGER: Logger = LoggerFactory.getLogger(FixBatchFiles.getClass)

  def apply(conf: Conf, params: Array[String])(implicit fhirClient: IGenericClient, s3Client: S3Client): ValidationResult[Boolean] = {

    val bucket = conf.aws.bucketName
    val bucketOutput = conf.aws.outputBucketName
    val bucketOutputPrefix = conf.aws.outputPrefix
    val dryRun = params.contains("--dryrun")

    val s3Files = CheckS3Data.ls(bucket, "")
    val s3MetaFiles = s3Files.filter(_.key.endsWith("metadata.json"))

    // just in case the pipeline gives us a wrong output bucket
    if (!conf.aws.outputBucketName.contains("download")) {
      throw new IllegalStateException(s"Output bucket isn't 'download' bucket: ${bucketOutput} check your pipeline")
    }

    val results = s3MetaFiles.map(meta => {
      try {
        val batchId = getBatchId(meta)
        if (batchId.contains("test_somatic_normal_part1")) { // TODO: remove this condition after testing
          val fullMeta = getFullMetadata(bucket, meta)
          if (fullMeta.isDefined) {
            fullMeta.get.analyses.foreach(analysis => {
              val aliquotId = analysis.labAliquotId
              val igvFiles = getIGVFiles(analysis)
              if (igvFiles.exists) {
                LOGGER.info(s"IGV found batchID: ${batchId} aliquotId: ${aliquotId} files: ${igvFiles}")
                val task = findTaskByAliquotId(batchId,aliquotId)
                if (task.isDefined) {
                  val taskId = task.get.getIdElement.getIdPart
                  LOGGER.info(s"- Task Found: ${taskId}")
                  if (!hasIGVFiles(task.get)) {
                    LOGGER.info(s"- IGV files are missing")
                    val (s3_seg_bw, s3_baf_bw, s3_roh_bed, s3_exome_bed) = prepareCopy(batchId, bucketOutputPrefix, igvFiles, s3Files)
                    val files : Seq[FileEntry] = Seq(s3_seg_bw, s3_baf_bw, s3_roh_bed, s3_exome_bed).filter(_ != null)
                    LOGGER.info(s"- Prepared files to copy: ${files.map(_.id).mkString(" ")}")
                    /* remaining steps
                    - create new DocumentReference with every IGV files
                    - add IGV part to the task with DocumentReference ID
                    - send BUNDLE for bothTask is an update and documentreference is a create
                    - throw error if any, but dont stop execution
                    - rollback if error copied files
                     */
                  } else {
                    LOGGER.info(s"- IGV files are good")
                  }
                } else {
                  LOGGER.warn((s"- No task found for batchId: $batchId and aliquotId ${aliquotId}"))
                }
                Thread.sleep(1000L) // don't spam FHIR too much, we have time
              } else {
                LOGGER.warn(s"No IGV files found for ${batchId} analysis ${analysis.labAliquotId}")
              }
            })
          } else {
            LOGGER.warn(s"Could not parse metadata for ${batchId}")
          }
        }
        Valid(meta.key)
      } catch {
        case e: Throwable => e.getMessage.invalid
      }
    })

    if (results.exists(_.isInvalid)) {
      val errorsList = results.filter(_.isInvalid).map(_.swap.getOrElse("")).mkString("\n")
      throw new IllegalStateException(errorsList)
    } else {
      if (dryRun) {

      }
    }

    Valid(true)
  }

  case class IGVFiles(seg_hw: String, roh: String, hard_filtered_baf: String, hyper_exome_hg38: String) {
    def exists: Boolean = !StringUtils.isAllBlank(seg_hw, roh, hard_filtered_baf, hyper_exome_hg38)
  }

  private def prepareCopy(batchId: String, prefix: String, igvFiles: IGVFiles, s3Files: List[RawFileEntry]) = {

    val uuid = UUID.randomUUID().toString

    def prepareFile(file: String, suffix: String) = {
      val key = s"$batchId/$file"
      val entry = s3Files.find(_.key.equals(key)).getOrElse(throw new IllegalStateException(s"Can't find $key in S3"))
      FileEntry(entry, s"$prefix/$uuid.$suffix", None, APPLICATION_OCTET_STREAM.getMimeType, attach(file))
    }

    val s3_seg_bw = Option(igvFiles.seg_hw).map(prepareFile(_,"seg.bw")).orNull
    val s3_baf_bw = Option(igvFiles.hard_filtered_baf).map(prepareFile(_,"baf.bw")).orNull
    val s3_roh_bed = Option(igvFiles.roh).map(prepareFile(_,"roh.bed")).orNull
    val s3_exome_bed = Option(igvFiles.hyper_exome_hg38).map(prepareFile(_,"exome.bed")).orNull

    (s3_seg_bw, s3_baf_bw, s3_roh_bed, s3_exome_bed)
  }

  private def hasIGVFiles(task: Task): Boolean = {
    task.getOutput.asScala.exists(_.getType.getCodingFirstRep.getCode.equals(IgvTrack.documentType))
  }

  private def findTaskByAliquotId(batchId: String,aliquotId: String)(implicit fhirClient: IGenericClient): Option[Task] = {
    val tasks = SomaticNormalImport.fetchFHIRTasksByAliquotIDs(Array(aliquotId))
    val filtered= tasks
      .filter(t => t.getCode.getCodingFirstRep.getCode.equals(EXOME_GERMLINE_ANALYSIS) || t.getCode.getCodingFirstRep.getCode.equals(EXTUM_ANALYSIS))
      .filter(t => t.getGroupIdentifier.getValue.equals(batchId))
    if (filtered.size > 1) {
      LOGGER.warn(s"- Found more than one task: ${tasks.size} for aliquotId ${aliquotId}")
      None
    } else {
      filtered.headOption
    }
  }

  private def getBatchId(key: RawFileEntry) = {
    key.key.split("/").head
  }

  private def getIGVFiles(analysis: Analysis): IGVFiles = {
    IGVFiles(
      analysis.files.seg_bw.orNull,
      analysis.files.roh_bed.orNull,
      analysis.files.hard_filtered_baf_bw.orNull,
      analysis.files.hyper_exome_hg38_bed.orNull
    )
  }

  private def getFullMetadata(bucket: String, meta: RawFileEntry)(implicit s3Client: S3Client): Option[FullMetadata] = {
    val metaContent = S3Utils.getContent(bucket, meta.key)(s3Client)
    val metaJson = Json.parse(metaContent)
    metaJson.validate[FullMetadata] match {
      case JsSuccess(m, _) => Some(m)
      case _ => None
    }
  }

}
