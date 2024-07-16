package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.SomaticNormalImport.{buildFileEntryID, formatDisplaySpecimen}
import bio.ferlab.clin.etl.{LOGGER, SomaticNormalImport, ValidationResult}
import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.{DR_CATEGORY, DR_FORMAT, DR_TYPE}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Extensions.FULL_SIZE
import bio.ferlab.clin.etl.fhir.{FhirUtils, IClinFhirClient}
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data.attach
import bio.ferlab.clin.etl.task.fileimport.model.TTask.{EXOME_GERMLINE_ANALYSIS, EXTUM_ANALYSIS, SOMATIC_NORMAL}
import bio.ferlab.clin.etl.task.fileimport.model.{Analysis, FileEntry, FullMetadata, IgvTrack, RawFileEntry, TBundle}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import cats.implicits.catsSyntaxValidatedId
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsSuccess, Json}
import software.amazon.awssdk.services.s3.S3Client
import cats.data.Validated.{Invalid, Valid}
import org.apache.commons.lang3.StringUtils
import org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model.{Attachment, Bundle, CodeableConcept, Coding, DecimalType, DocumentReference, IdType, Reference, Task}

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
        //if (batchId.contains("test_somatic_normal_part1")) { // TODO: remove this condition after testing
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
                    val files : Seq[FileEntry] = Seq(s3_seg_bw, s3_baf_bw, s3_roh_bed, s3_exome_bed).filter(_ .isDefined).map(_.get)
                    LOGGER.info(s"- Prepared files to copy: ${files.map(_.id).mkString(" ")}")
                    val (documentReference, updatedTask) = buildIGVDocAndUpdateTask(conf.ferload.cleanedUrl, task.get, s3_seg_bw, s3_baf_bw, s3_roh_bed, s3_exome_bed)

                    var res: Seq[BundleEntryComponent] = Seq()
                    res = res ++ FhirUtils.bundleCreate(Seq(documentReference)) ++ FhirUtils.bundleUpdate(Seq(updatedTask))
                    val bundle = TBundle(res.toList)

                    if (!dryRun) {
                      submit(bundle, files, conf)
                    } else {
                      LOGGER.info("Request:\n" + bundle.print())
                    }

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
            throw new IllegalStateException(s"Could not parse metadata for ${batchId}")
          }
        //}
        Valid(true)
      } catch {
        case e: Throwable => e.getMessage.invalid
      }
    })

    if (results.exists(_.isInvalid)) {
      val errorsList = results.filter(_.isInvalid).map(_.swap.getOrElse("")).mkString("\n")
      throw new IllegalStateException(errorsList)
    } else {
      Valid(true)
    }
  }

  case class IGVFiles(seg_hw: String, roh: String, hard_filtered_baf: String, hyper_exome_hg38: String) {
    def exists: Boolean = !StringUtils.isAllBlank(seg_hw, roh, hard_filtered_baf, hyper_exome_hg38)
  }

  private def submit(bundle: TBundle, files: Seq[FileEntry], conf: Conf)(implicit s3Client: S3Client, fhirClient:IGenericClient) = {
    try {
      if (conf.aws.copyFileMode.equals("async")) {
        CheckS3Data.copyFilesAsync(files, conf.aws.outputBucketName)(conf.aws)
      } else {
        CheckS3Data.copyFiles(files, conf.aws.outputBucketName)
      }

      val result = bundle.save()
      if (result.isInvalid) {
        CheckS3Data.revert(files, conf.aws.outputBucketName)
      } else {
        LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head))
      }
    } catch {
      case e: Throwable => {
        CheckS3Data.revert(files, conf.aws.outputBucketName)
        throw e
      }
    }
  }

  private def buildIGVDocAndUpdateTask(ferloadURL: String, task: Task,
                                        s3_seg_bw: Option[FileEntry],
                                        s3_baf_bw : Option[FileEntry],
                                        s3_roh_bed : Option[FileEntry],
                                        s3_exome_bed: Option[FileEntry]) = {

    def addContext(doc: DocumentReference, task: Task) = {
      val specimen = task.getInputFirstRep.getValue.asInstanceOf[Reference]
      doc.getContext.addRelated().setReference(specimen.getReference).setDisplay(formatDisplaySpecimen(specimen))
    }

    def addContent(doc: DocumentReference, file: FileEntry, formatType: String) = {
      val content = doc.addContent()
      val attachment = new Attachment()
      attachment.addExtension(FULL_SIZE, new DecimalType(file.size))
      attachment.setContentType(APPLICATION_OCTET_STREAM.getMimeType)
      attachment.setUrl(s"$ferloadURL/${file.id}")
      attachment.setTitle(file.filename)
      content.setAttachment(attachment)
      val format = new Coding()
      format.setSystem(DR_FORMAT)
      format.setCode(formatType)
      content.setFormat(format)
    }

    val doc = new DocumentReference
    doc.setId(IdType.newRandomUuid())
    doc.setStatus(DocumentReferenceStatus.CURRENT)
    doc.setType(new CodeableConcept(new Coding().setSystem(DR_TYPE).setCode("IGV")))
    doc.addCategory(new CodeableConcept(new Coding().setSystem(DR_CATEGORY).setCode("GENO")))
    doc.setSubject(task.getFor)

    s3_seg_bw.foreach(s3File => addContent(doc, s3File, "BW"))
    s3_baf_bw.foreach(s3File => addContent(doc, s3File, "BW"))
    s3_roh_bed.foreach(s3File => addContent(doc, s3File, "BED"))
    s3_exome_bed.foreach(s3File => addContent(doc, s3File, "BED"))

    addContext(doc, task)

    // add DocumentReference as output of the Task
    val o1 = task.addOutput()
    o1.getType.addCoding(doc.getType.getCodingFirstRep)
    o1.setValue(new Reference().setReference(doc.getId))

    (doc, task)
  }

  private def prepareCopy(batchId: String, prefix: String, igvFiles: IGVFiles, s3Files: List[RawFileEntry]) = {

    val uuid = UUID.randomUUID().toString

    def prepareFile(file: String, suffix: String) = {
      val key = s"$batchId/$file"
      val entry = s3Files.find(_.key.equals(key)).getOrElse(throw new IllegalStateException(s"Can't find $key in S3"))
      FileEntry(entry, buildFileEntryID(prefix, uuid)+"."+suffix, None, APPLICATION_OCTET_STREAM.getMimeType, attach(file))
    }

    val s3_seg_bw = Option(igvFiles.seg_hw).map(prepareFile(_,"seg.bw"))
    val s3_baf_bw = Option(igvFiles.hard_filtered_baf).map(prepareFile(_,"baf.bw"))
    val s3_roh_bed = Option(igvFiles.roh).map(prepareFile(_,"roh.bed"))
    val s3_exome_bed = Option(igvFiles.hyper_exome_hg38).map(prepareFile(_,"exome.bed"))

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
