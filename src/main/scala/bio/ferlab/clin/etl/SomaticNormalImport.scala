package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.{AWSConf, Conf}
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems.{ANALYSIS_TYPE, DR_CATEGORY, DR_FORMAT, DR_TYPE}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Extensions.{FULL_SIZE, SEQUENCING_EXPERIMENT}
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.scripts.SwitchSpecimenValues
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data.attach
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, RawFileEntry, TBundle}
import bio.ferlab.clin.etl.task.fileimport.model.TTask.{EXOME_GERMLINE_ANALYSIS, EXTUM_ANALYSIS}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import org.apache.commons.io.IOUtils
import org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM
import org.hl7.fhir.r4.model.Bundle.{BundleEntryComponent, SearchEntryMode}
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model.Task.ParameterComponent
import org.hl7.fhir.r4.model._
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.util.{Scanner, UUID}
import java.util.zip.GZIPInputStream
import scala.Seq
import scala.collection.JavaConverters.asScalaBufferConverter

object SomaticNormalImport extends App {

  val LOGGER: Logger = LoggerFactory.getLogger(SwitchSpecimenValues.getClass)

  withSystemExit {
    withLog {
      withConf { conf =>
        withExceptions {
          val batch_id = if (args.length > 0) args.head else throw new IllegalArgumentException(s"Missing batch_id")
          val params = if (args.length > 1) args.tail else Array.empty[String]
          importBatch(batch_id, params)(conf)
        }
      }
    }
  }

  private def importBatch(batchId: String, params : Array[String])(implicit conf: Conf) = {
    implicit val s3Client = buildS3Client(conf.aws)
    val (_, fhirClient) = buildFhirClients(conf.fhir, conf.keycloak)
    val bucket = conf.aws.bucketName
    val bucketOutputPrefix = conf.aws.outputPrefix
    val dryRun = params.contains("--dryrun")

    val s3VCFFiles = CheckS3Data.ls(bucket, batchId)

    var res: Seq[BundleEntryComponent] = Seq()
    var files : Seq[FileEntry] = Seq()

    s3VCFFiles
      .filter(f => f.filename.endsWith(".vcf.gz"))  // keep only VCF files
      .foreach(s3VCF => {
        // find associated TBI (is it optional? could be ... let make it mandatory for now)
        val s3VCFTbi = s3VCFFiles.find(f => f.filename.equals(s3VCF.filename+".tbi")).getOrElse(throw new IllegalStateException(s"Cant find TBI file for: ${s3VCF.filename}"))

        val aliquotIDs = extractAliquotIDs(bucket, s3VCF.key)
        LOGGER.info(s"${s3VCF.filename} contains aliquot IDs: ${aliquotIDs.mkString(" ")}")

        val (taskGermline, taskSomatic) = findFhirTasks(aliquotIDs)(fhirClient)
        validateTasks(s3VCF, taskGermline, taskSomatic)

        val (copiedVCF, copiedTBI) = prepareCopy(bucketOutputPrefix, s3VCF, s3VCFTbi)
        files = files ++ Seq(copiedVCF, copiedTBI)

        val documentReference = buildDocumentReference(conf.ferload.cleanedUrl, taskGermline, taskSomatic, copiedVCF, copiedTBI)
        val task = buildTask(taskGermline, taskSomatic)
        res = res ++ FhirUtils.bundleCreate(Seq(documentReference, task))
      })

    val bundle = TBundle(res.toList)
    LOGGER.info("Request:\n" + bundle.print()(fhirClient))
    if (dryRun) {
      Valid(true)
    } else {
      /*val result = bundle.save()(fhirClient)
      LOGGER.info("Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
      Valid(result.isValid)*/
      Valid(true)
    }

    /*
    TODO
    - lister les fichiers VCF S3
    - pour chaque VCF
    -- extraire aliquots IDs
    -- optionel logger un warning si les aliqots ne match pas le nom du fichier
    -- si le vcf.gz n'est pas valide ou ne contient pas d'aliquots ou n'est pas un vcf alors planter
    -- demander a FHIR les Tasks de ces deux aliquots
    -- implementer l'algo de l'analyse => trouverTask
    --- rajouter une petite subtilite => si TNEBA existe deja alors ne rien faire
    -- creer DocumentReference + Task (ne pas envoyer vers FHIR, on va tout mettre dans un bundle pour la fin)
    - on fait un seul POST vers FHIR
    */
  }

  private def prepareCopy(prefix: String, vcf: RawFileEntry, tbi: RawFileEntry) = {
    val uuid = UUID.randomUUID().toString
    (FileEntry(vcf, s"$prefix/$uuid", None, APPLICATION_OCTET_STREAM.getMimeType, attach(vcf.filename)),
      FileEntry(tbi, s"$prefix/$uuid.tbi", None, APPLICATION_OCTET_STREAM.getMimeType, attach(vcf.filename)))
  }

  private def buildDocumentReference(ferloadURL: String, taskGermline: Task, taskSomatic: Task, copiedVCF: FileEntry, copiedTBI: FileEntry) = {

    def addContext(doc: DocumentReference, task: Task, displayType: String) = {
      val specimen = task.getInputFirstRep.getValue.asInstanceOf[Reference]
      doc.getContext.addRelated().setReference(specimen.getReference).setDisplay(s"Submitter $displayType Sample ID: ${specimen.getDisplay.split(":")(1).trim}")
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
    doc.setStatus(DocumentReferenceStatus.CURRENT)
    doc.setType(new CodeableConcept(new Coding().setSystem(DR_TYPE).setCode("SSNV")))
    doc.addCategory(new CodeableConcept(new Coding().setSystem(DR_CATEGORY).setCode("GEMO")))
    doc.setSubject(new Reference(taskSomatic.getFor.getReference))

    addContent(doc, copiedVCF, "VCF")
    addContent(doc, copiedTBI, "TBI")

    addContext(doc, taskGermline, "Normal")
    addContext(doc, taskSomatic, "Tumor")

    doc
  }

  private def buildTask(taskGermline: Task, taskSomatic: Task) = {
    val task = new Task()
    task.setCode(new CodeableConcept(new Coding().setSystem(ANALYSIS_TYPE).setCode("TNEBA")))
    task
  }

  private def validateTasks(s3VCF: RawFileEntry, taskGermline: Task, taskSomatic: Task) = {
    val file = s3VCF.filename
    val aliquotIDGermline = extractAliquotID(taskGermline).get
    val aliquotIDSomatic = extractAliquotID(taskSomatic).get

    if (!file.startsWith(s"T-$aliquotIDSomatic.N-$aliquotIDGermline")) {
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

  private def findFhirTasks(aliquotIDs: Array[String])(fhirClient: IGenericClient) = {

    def searchTask(offset: Int, count: Int = 100) = {
      val res = fhirClient.search().forResource(classOf[Task]).count(count).offset(offset).returnBundle(classOf[Bundle]).execute()
      res.getEntry.asScala.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Task] }
    }

    def hasAnyAliquotIds(task: Task, aliquotIDs: Array[String]) = {
      extractAliquotID(task).filter(aliquotIDs.contains)
    }

    var taskGermline: Option[Task] = None
    var taskSomatic: Option[Task] = None

    var currentOffset = 0
    var searchCompleted = false

    while(!searchCompleted) {
      val tasks = searchTask(currentOffset)
      currentOffset += tasks.length
      tasks.foreach(task => {
        val matchingAliquot = hasAnyAliquotIds(task, aliquotIDs)
        if(matchingAliquot.isDefined){
          val aliquot = matchingAliquot.get
          task.getCode.getCodingFirstRep.getCode match {
            case EXOME_GERMLINE_ANALYSIS => LOGGER.info(s"Found Task Germline id: ${task.getIdElement.getIdPart} with aliquot: $aliquot"); taskGermline = Some(task)
            case EXTUM_ANALYSIS => LOGGER.info(s"Found Task Somatic id: ${task.getIdElement.getIdPart} with aliquot: $aliquot"); taskSomatic = Some(task)
            case s: Any => throw new IllegalStateException(s"Found unknown Task with type: $s for aliquot ID: $aliquot")
          }
        }
      })
      searchCompleted = (taskGermline.isDefined && taskSomatic.isDefined) || tasks.isEmpty
    }

    if (taskGermline.isEmpty || taskSomatic.isEmpty) {
      throw new IllegalStateException(s"Can't find all required FHIR Tasks for aliquot IDs ${aliquotIDs.mkString(" ")}")
    }

    (taskGermline.get, taskSomatic.get)
  }

  private def extractAliquotIDs(bucket: String, key: String)(implicit s3Client: S3Client) = {
    val vcfInputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
    val vcfReader = new Scanner(new GZIPInputStream(vcfInputStream))
    var aliquots: Option[Array[String]] = None
    while(vcfReader.hasNext && aliquots.isEmpty) {
      val line = vcfReader.nextLine();
      if (line.startsWith("#CHROM")) {
        aliquots = Some(line.replace("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t", "").split("\t"))
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
