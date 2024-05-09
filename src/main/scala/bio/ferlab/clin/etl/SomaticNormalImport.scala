package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.{ANALYSIS_TYPE, DR_CATEGORY, DR_FORMAT, DR_TYPE}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Extensions.{FULL_SIZE, SEQUENCING_EXPERIMENT, WORKFLOW}
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.scripts.SwitchSpecimenValues
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data.attach
import bio.ferlab.clin.etl.task.fileimport.model.TTask.{EXOME_GERMLINE_ANALYSIS, EXTUM_ANALYSIS, SOMATIC_NORMAL}
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, RawFileEntry, TBundle}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.{Invalid, Valid}
import cats.implicits.catsSyntaxValidatedId
import org.apache.commons.io.IOUtils
import org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model.Task.TaskStatus
import org.hl7.fhir.r4.model._
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.util.zip.GZIPInputStream
import java.util.{Scanner, UUID}
import scala.collection.JavaConverters.asScalaBufferConverter

object SomaticNormalImport extends App {

  withSystemExit {
    withExceptions {
      withConf { conf =>
         withLog {
          val batch_id = if (args.length > 0) args.head else throw new IllegalArgumentException(s"Missing batch_id")
          val params = if (args.length > 1) args.tail else Array.empty[String]
          SomaticNormalImport(batch_id, params)(conf)
        }
      }
    }
  }

  def apply(batchId: String, params : Array[String])(implicit conf: Conf): ValidationResult[Any] = {
    val LOGGER: Logger = LoggerFactory.getLogger(SwitchSpecimenValues.getClass)

    implicit val s3Client = buildS3Client(conf.aws)
    val (_, fhirClient) = buildFhirClients(conf.fhir, conf.keycloak)
    val bucket = conf.aws.bucketName
    val bucketOutputPrefix = conf.aws.outputPrefix
    val dryRun = params.contains("--dryrun")

    val s3Files = CheckS3Data.ls(bucket, batchId)
    val s3VCFFiles = s3Files.filter(f => f.filename.endsWith(".vcf.gz"))  // keep only VCF files

    if (s3VCFFiles.isEmpty) { // wrong batch id folder ??
      throw new IllegalStateException(s"No VCF files found in: $batchId")
    }

    var res: Seq[BundleEntryComponent] = Seq()
    var files : Seq[FileEntry] = Seq()

    val results = s3VCFFiles.map(s3VCF => {
      try {
        // find associated TBI (is it optional? could be ... let make it mandatory for now)
        val s3VCFTbi = s3Files.find(f => f.filename.equals(s3VCF.filename+".tbi")).getOrElse(throw new IllegalStateException(s"Cant find TBI file for: ${s3VCF.key}"))

        val aliquotIDs = extractAliquotIDs(bucket, s3VCF.key)
        LOGGER.info(s"${s3VCF.filename} contains aliquot IDs: ${aliquotIDs.mkString(" ")}")

        val FHIRTasksByAliquotIDs = fetchFHIRTasksByAliquotIDs(aliquotIDs)(fhirClient)
        val (taskGermline, taskSomatic, existingTNBEATask) = findFhirTasks(FHIRTasksByAliquotIDs, aliquotIDs)

        if (existingTNBEATask.isEmpty) {

          validateTasks(s3VCF, taskGermline, taskSomatic)

          val (copiedVCF, copiedTBI) = prepareCopy(bucketOutputPrefix, s3VCF, s3VCFTbi)
          files = files ++ Seq(copiedVCF, copiedTBI)

          val documentReference = buildDocumentReference(conf.ferload.cleanedUrl, taskGermline, taskSomatic, copiedVCF, copiedTBI)
          val task = buildTask(batchId, taskGermline, taskSomatic)
          res = res ++ FhirUtils.bundleCreate(Seq(documentReference, task))
        }

        s3VCF.key.valid
      } catch {
        // catch all errors
        case e: Throwable => e.getMessage.invalid
      }
    })

    if (results.exists(_.isInvalid)) {
      val errorsList = results.filter(_.isInvalid).map(_.swap.getOrElse("")).mkString("\n")
      throw new IllegalStateException(errorsList)
    } else {

      val bundle = TBundle(res.toList)
      LOGGER.info("Request:\n" + bundle.print()(fhirClient))

      if (dryRun) {
        Valid(true)
      } else {
        submit(files, bundle)(s3Client, fhirClient, conf)
      }
    }
  }

  private def submit(files: Seq[FileEntry], bundle: TBundle)(implicit s3Client: S3Client, fhirClient:IGenericClient, conf: Conf) = {
    try {
      if (files.nonEmpty) {
        if (conf.aws.copyFileMode.equals("async")) {
          CheckS3Data.copyFilesAsync(files, conf.aws.outputBucketName)(conf.aws)
        } else {
          CheckS3Data.copyFiles(files, conf.aws.outputBucketName)(s3Client)
        }

        val result = bundle.save()(fhirClient)

        if (result.isInvalid) {
          CheckS3Data.revert(files, conf.aws.outputBucketName)
        } else {
          LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
        }
        result
      } else {
        Valid("Nothing to submit")
      }
    } catch {
      case e: Throwable => {
        CheckS3Data.revert(files, conf.aws.outputBucketName)
        throw e
      }
    }
  }

  private def prepareCopy(prefix: String, vcf: RawFileEntry, tbi: RawFileEntry) = {
    val uuid = UUID.randomUUID().toString
    (FileEntry(vcf, s"$prefix/$uuid", None, APPLICATION_OCTET_STREAM.getMimeType, attach(vcf.filename)),
      FileEntry(tbi, s"$prefix/$uuid.tbi", None, APPLICATION_OCTET_STREAM.getMimeType, attach(vcf.filename)))
  }

  private def formatDisplaySpecimen(specimen: Reference, displayType: String = "") = {
    s"Submitter $displayType Sample ID: ${specimen.getDisplay.split(":")(1).trim}"
  }

  private def buildDocumentReference(ferloadURL: String, taskGermline: Task, taskSomatic: Task, copiedVCF: FileEntry, copiedTBI: FileEntry) = {

    def addContext(doc: DocumentReference, task: Task, displayType: String) = {
      val specimen = task.getInputFirstRep.getValue.asInstanceOf[Reference]
      doc.getContext.addRelated().setReference(specimen.getReference).setDisplay(formatDisplaySpecimen(specimen, displayType))
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
    doc.setType(new CodeableConcept(new Coding().setSystem(DR_TYPE).setCode("SSNV")))
    doc.addCategory(new CodeableConcept(new Coding().setSystem(DR_CATEGORY).setCode("GENO")))
    doc.setSubject(taskSomatic.getFor)

    addContent(doc, copiedVCF, "VCF")
    addContent(doc, copiedTBI, "TBI")

    addContext(doc, taskGermline, "Normal")
    addContext(doc, taskSomatic, "Tumor")

    doc
  }

  private def buildTask(batchId: String,taskGermline: Task, taskSomatic: Task) = {

    def addInput(task: Task, inputTask: Task, inputType: String) = {
      val i1 = task.addInput()
      val specimen = inputTask.getInputFirstRep.getValue.asInstanceOf[Reference]
      i1.setType(new CodeableConcept().setText(s"Analysed ${inputType.toLowerCase} sample"))
      i1.setValue(new Reference().setReference(specimen.getReference).setDisplay(formatDisplaySpecimen(specimen)))

      val i2 = task.addInput()
      i2.setType(new CodeableConcept().setText(s"$inputType Exome Bioinformatic Analysis"))
      i2.setValue(new Reference().setReference(s"Task/${inputTask.getIdElement.getIdPart}"))
    }

    val task = new Task()
    task.setId(IdType.newRandomUuid())
    task.setCode(new CodeableConcept(new Coding().setSystem(ANALYSIS_TYPE).setCode(SOMATIC_NORMAL)))
    task.addExtension(taskSomatic.getExtensionByUrl(WORKFLOW))
    task.addExtension(taskSomatic.getExtensionByUrl(SEQUENCING_EXPERIMENT))
    task.addBasedOn(taskSomatic.getBasedOnFirstRep)
    task.setStatus(TaskStatus.COMPLETED)
    task.setIntent(taskSomatic.getIntent)
    task.setPriority(taskSomatic.getPriority)
    task.setFocus(taskSomatic.getFocus)
    task.setFor(taskSomatic.getFor)
    task.setRequester(taskSomatic.getRequester)
    task.setOwner(taskSomatic.getOwner)
    task.getGroupIdentifier.setValue(batchId)
    //task.setAuthoredOn(taskSomatic.getAuthoredOn)
    addInput(task, taskGermline, "Normal")
    addInput(task, taskSomatic, "Tumor")
    task
  }

  private def validateTasks(s3VCF: RawFileEntry, taskGermline: Task, taskSomatic: Task): Unit = {
    val file = s3VCF.filename
    val aliquotIDGermline = extractAliquotID(taskGermline).get
    val aliquotIDSomatic = extractAliquotID(taskSomatic).get

    if (!file.startsWith(s"$aliquotIDSomatic.$aliquotIDGermline")) {
      throw new IllegalStateException(s"$file file name doesn't match aliquot IDs inside")
    }

    val patientGermline = taskGermline.getFor.getReference
    val patientSomatic = taskSomatic.getFor.getReference
    if (!patientGermline.equals(patientSomatic)) {
      throw new IllegalStateException(s"Tasks don't have the same patient for reference germline => $patientGermline and somatic => $patientSomatic")
    }
  }

  private def extractAliquotID(task : Task) = {
    Some(task.getExtensionByUrl(SEQUENCING_EXPERIMENT))
      .map(_.getExtensionByUrl("labAliquotId"))
      .map(_.getValue)
      .map(_.toString)
  }

  private def fetchFHIRTasksByAliquotIDs(aliquotIDs: Array[String])(implicit fhirClient: IGenericClient): Seq[Task] = {
    val res = fhirClient.search.byUrl(s"Task?aliquotid=${aliquotIDs.mkString(",")}").returnBundle(classOf[Bundle]).execute
    res.getEntry.asScala.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Task] }
  }

  private def findFhirTasks(allFHIRTasks: Seq[Task], aliquotIDs: Array[String]) = {

    var taskGermline: Option[Task] = None
    var taskSomatic: Option[Task] = None
    var existingTNEBATask: Option[Task] = None

    allFHIRTasks.foreach(task => {
      val matchingAliquot =  extractAliquotID(task).filter(aliquotIDs.contains)
      if(matchingAliquot.isDefined){
        val aliquot = matchingAliquot.get
        task.getCode.getCodingFirstRep.getCode match {
          case EXOME_GERMLINE_ANALYSIS => {
            LOGGER.info(s"Found FHIR Task Germline id: ${task.getIdElement.getIdPart} with aliquot: $aliquot")
            taskGermline = Some(task)
          }
          case EXTUM_ANALYSIS => {
            LOGGER.info(s"Found FHIR Task Somatic id: ${task.getIdElement.getIdPart} with aliquot: $aliquot")
            taskSomatic = Some(task)
          }
          case SOMATIC_NORMAL => {
            LOGGER.info(s"Existing FHIR TNEBA Task id: ${task.getIdElement.getIdPart} with aliquot: $aliquot")
            existingTNEBATask = Some(task)
          }
        }
      }
    })

    if (existingTNEBATask.isEmpty && (taskGermline.isEmpty || taskSomatic.isEmpty)) {
      throw new IllegalStateException(s"Can't find all required FHIR Tasks for aliquot IDs: ${aliquotIDs.mkString(" ")}")
    }

    (taskGermline.get, taskSomatic.get, existingTNEBATask)
  }

  private def extractAliquotIDs(bucket: String, key: String)(implicit s3Client: S3Client) = {
    val vcfInputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
    val vcfReader = new Scanner(new GZIPInputStream(vcfInputStream))
    var aliquots: Option[Array[String]] = None
    while(vcfReader.hasNext && aliquots.isEmpty) {
      val line = vcfReader.nextLine();
      if (line.startsWith("#CHROM")) {
        aliquots = Some(line.replace("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t", "").split("\t"))
        vcfInputStream.abort();
      }
    }
    IOUtils.closeQuietly(vcfReader, vcfInputStream)
    aliquots
      .filter(ids => ids != null && ids.nonEmpty)
      .orElse(throw new IllegalStateException(s"$key no aliquot IDs found"))
      .filter(ids => ids.length == 2)
      .orElse(throw new IllegalStateException(s"$key contains more than 2 aliquot IDs"))
      .get
  }
}
