package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.Scripts.args
import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.scripts.SwitchSpecimenValues
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data
import bio.ferlab.clin.etl.task.fileimport.model.TTask.{EXOME_GERMLINE_ANALYSIS, EXTUM_ANALYSIS}
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.apache.commons.io.IOUtils
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus
import org.hl7.fhir.r4.model.{Bundle, CodeableConcept, Coding, DocumentReference, Task}
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, ListObjectsRequest}

import java.util.Scanner
import java.util.zip.GZIPInputStream
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
    val dryRun = params.contains("--dryrun")

    val s3VCFFiles = CheckS3Data.ls(bucket, batchId)

    s3VCFFiles.foreach(s3VCF => {
      val aliquotIDs = extractAliquotIDs(bucket, s3VCF.key)
      compareWithFileName(s3VCF.filename, aliquotIDs)
      val (taskGermline, taskSomatic) = findFhirTasks(aliquotIDs)(fhirClient)
      compareTasksPatient(taskGermline, taskSomatic)
      val documentReference = buildDocumentReference()
    })

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

  private def buildDocumentReference() = {
    val doc = new DocumentReference
    doc.setStatus(DocumentReferenceStatus.CURRENT)
    doc.setType(new CodeableConcept(new Coding().setSystem("http://fhir.cqgc.ferlab.bio/CodeSystem/data-type").setCode("SSNV")))
    doc
  }

  private def compareTasksPatient(taskGermline: Task, taskSomatic: Task) = {
    val patientGermline = taskGermline.getFor.getReference
    val patientSomatic = taskSomatic.getFor.getReference
    if (!patientGermline.equals(patientSomatic)) {
      throw new IllegalStateException(s"Tasks don't have the same patient for reference germline => $patientGermline and somatic => $patientSomatic")
    }
  }

  private def findFhirTasks(aliquotIDs: Array[String])(fhirClient: IGenericClient) = {

    def searchTask(offset: Int, count: Int = 100) = {
      val res = fhirClient.search().forResource(classOf[Task]).count(count).offset(offset).returnBundle(classOf[Bundle]).execute()
      res.getEntry.asScala.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Task] }
    }

    def hasAnyAliquotIds(task: Task, aliquotIDs: Array[String]) = {
      val aliquot = Some(task.getExtensionByUrl("http://fhir.cqgc.ferlab.bio/StructureDefinition/sequencing-experiment"))
        .map(_.getExtensionByUrl("labAliquotId"))
        .map(_.getValue)
        .map(_.toString)
        .orNull
      Some(aliquot).filter(aliquotIDs.contains)
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

  private def compareWithFileName(file: String, aliquotIDs: Array[String]): Unit = {
    // we don't mind the aliquots order
    def validFileName(somaticAliquot: String, germlineAliquot: String): Boolean = {
      file.startsWith(s"T-$somaticAliquot.N-$germlineAliquot")
    }
    LOGGER.info(s"$file contains aliquot IDs: ${aliquotIDs.mkString(" ")}")
    if (!validFileName(aliquotIDs(0), aliquotIDs(1)) && !validFileName(aliquotIDs(1), aliquotIDs(0))) {
      throw new IllegalStateException(s"$file file name doesn't match aliquot IDs inside")
    }
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
