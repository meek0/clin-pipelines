package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.CheckS3Data
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, TBundle}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.hl7.fhir.r4.model.{Bundle, DocumentReference}
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Client}

import scala.annotation.tailrec

case object AddMissingFileExtensions {

  val LOGGER: Logger = LoggerFactory.getLogger(FixFerloadURLs.getClass)
  final val UrlRegex: String = "(https?:\\/\\/)([^\\/]+)"

  def apply(conf: Conf, params: Array[String])(implicit fhirClient: IGenericClient, s3Client: S3Client): ValidationResult[Boolean] = {

    // Files to modify are in output bucket
    val bucket = conf.aws.outputBucketName
    val dryRun = params.contains("--dryrun")

    val asyncS3Client: Option[S3AsyncClient] = if (conf.aws.copyFileMode.equals("async"))
      Some(S3Utils.buildAsyncS3Client(conf.aws)) else None

    // just in case the pipeline gives us a wrong output bucket
    if (!bucket.contains("download")) {
      throw new IllegalStateException(s"Output bucket isn't 'download' bucket: $bucket check your pipeline")
    }

    @tailrec
    def addMissingFileExtensions(offset: Int = 0, size: Int = 100): Unit = {
      LOGGER.info(s"Fetching Docs from offset $offset with size $size")
      val results: Bundle = fhirClient.search().forResource(classOf[DocumentReference])
        .offset(offset)
        .count(size)
        .encodedJson()
        .returnBundle(classOf[Bundle]).execute()

      if (results.getEntry.size() > 0) {
        var fhirRessources: Seq[BundleEntryComponent] = Seq()
        var s3Files: Seq[FileEntry] = Seq()
        results.getEntry.forEach(entry => {
          val doc = entry.getResource.asInstanceOf[DocumentReference]
          var updated = false
          doc.getContent.forEach(c => {
            val attachment = c.getAttachment
            val fileUrl = attachment.getUrl
            val newFileUrl: Option[String] = c.getFormat.getCode match {
              case "CRAM" => getNewFileUrl(fileUrl, targetExtension = ".cram")
              case "CRAI" => getNewFileUrl(fileUrl, targetExtension = ".cram.crai", existingExtension = Some(".crai"))
              case "VCF" => getNewFileUrl(fileUrl, targetExtension = ".vcf.gz")
              case "TBI" => getNewFileUrl(fileUrl, targetExtension = ".vcf.gz.tbi", existingExtension = Some(".tbi"))
              case "TGZ" => getNewFileUrl(fileUrl, targetExtension = ".tgz")
              case "PNG" => getNewFileUrl(fileUrl, targetExtension = ".png")
              case "CSV" => getNewFileUrl(fileUrl, targetExtension = ".csv")
              case "JSON" => getNewFileUrl(fileUrl, targetExtension = ".json")
              case _ => None
            }

            if (newFileUrl.isDefined) {
              LOGGER.info(s"Replacing URL for ${doc.getIdElement.getIdPart} : ${attachment.getUrl} => ${newFileUrl.get}")
              c.getAttachment.setUrl(newFileUrl.get)

              val sourceKey = fileUrl.replaceAll(UrlRegex, "")
              val destinationKey = newFileUrl.get.replaceAll(UrlRegex, "")

              LOGGER.info(s"Moving file $sourceKey to $destinationKey in bucket $bucket")
              val s3File = prepareMove(bucket, sourceKey, destinationKey)
              s3Files = s3Files :+ s3File
              updated = true
            }
          })
          if (updated) {
            fhirRessources = fhirRessources ++ FhirUtils.bundleUpdate(Seq(doc))
          }
        })
        if (!dryRun && fhirRessources.nonEmpty) {
          // Update FHIR documents
          val bundle = TBundle(fhirRessources.toList)
          LOGGER.info("FHIR Request:\n" + bundle.print())
          val result = bundle.save()
          if (result.isValid) {
            LOGGER.info("FHIR Response :\n" + FhirUtils.toJson(result.toList.head)(fhirClient))
          }
          // Move S3 files
          asyncS3Client match {
            case Some(client) => CheckS3Data.moveFilesAsync(s3Files, bucket)(client)
            case None => CheckS3Data.moveFiles(s3Files, bucket)
          }
        }
        Thread.sleep(1000L) // don't spam FHIR too much, we have time
        addMissingFileExtensions(offset + size, size)
      }
    }

    addMissingFileExtensions()
    Valid(true)
  }

  private def getNewFileUrl(fileUrl: String,
                            targetExtension: String,
                            existingExtension: Option[String] = None): Option[String] = {
    if (fileUrl.endsWith(targetExtension)) None
    else {
      val ext = existingExtension.getOrElse("")
      Some(fileUrl.stripSuffix(ext) + targetExtension)
    }
  }

  private def prepareMove(bucket: String, sourceKey: String, destinationKey: String)
                         (implicit s3Client: S3Client): FileEntry = {
    val headRequest = HeadObjectRequest.builder()
      .bucket(bucket)
      .key(sourceKey)
      .build()

    val objectMetadata = s3Client.headObject(headRequest)
    FileEntry(bucket, key = sourceKey, md5 = None, size = objectMetadata.contentLength(), id = destinationKey,
      contentType = objectMetadata.contentType(), contentDisposition = objectMetadata.contentDisposition())
  }

}
