package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.isValid
import bio.ferlab.clin.etl.model.{FileEntry, Metadata, RawFileEntry}
import cats.data.ValidatedNel
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CheckS3Data {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def ls(bucket: String, prefix: String, maxKeys: Int = 4500)(implicit s3Client: S3Client): List[RawFileEntry] = {
    val lsRequest = ListObjectsV2Request.builder().bucket(bucket).maxKeys(maxKeys).prefix(prefix).build()

    nextBatch(s3Client, s3Client.listObjectsV2(lsRequest), maxKeys)
  }

  @tailrec
  private def nextBatch(s3Client: S3Client, listing: ListObjectsV2Response, maxKeys: Int, objects: List[RawFileEntry] = Nil): List[RawFileEntry] = {
    val pageKeys = listing.contents().asScala.map(o => RawFileEntry(listing.name(), o.key(), o.eTag().replace("\"", ""), o.size())).toList

    if (listing.isTruncated) {
      val nextRequest = ListObjectsV2Request.builder().bucket(listing.name).prefix(listing.prefix()).continuationToken(listing.nextContinuationToken()).build()
      nextBatch(s3Client, s3Client.listObjectsV2(nextRequest), maxKeys, pageKeys ::: objects)
    } else
      pageKeys ::: objects
  }

  def validateFileEntries(rawFileEntries: Seq[RawFileEntry], fileEntries: Seq[FileEntry]): ValidatedNel[String, Seq[FileEntry]] = {
    println("################# Validate File entries ##################")
    val fileEntriesNotInAnalysis = rawFileEntries.filterNot(r => fileEntries.exists(f => f.key == r.key))
    val errorFilesNotExist = fileEntriesNotInAnalysis.map(f => s"File ${f.filename} not found in metadata JSON file.")
    isValid(fileEntries, errorFilesNotExist)
  }

  def loadRawFileEntries(bucket: String, prefix: String)(implicit s3Client: S3Client): Seq[RawFileEntry] = {
    val fileEntries = ls(bucket, prefix)
      .filter(f => f.filename != "" && f.filename != "_SUCCESS" && f.filename != "metadata.json" && !f.filename.toLowerCase().startsWith("combined_vcf"))
    fileEntries
  }


  def loadFileEntries(m: Metadata, fileEntries: Seq[RawFileEntry], generateId: () => String = () => UUID.randomUUID().toString): Seq[FileEntry] = {

    val mapOfIds = m.analyses.flatMap { a =>
      val cramId: String = generateId()
      val craiId: String = generateId()
      val vcfId: String = generateId()
      val tbiId: String = generateId()
      val qcId: String = generateId()

      Seq(
        a.files.cram -> (cramId, "application/octet-stream", s""""attachment; filename="${a.files.cram}"""""),
        a.files.crai -> (craiId, "application/octet-stream", s""""attachment; filename="${a.files.crai}"""""),
        a.files.vcf -> (vcfId, "application/octet-stream", s""""attachment; filename="${a.files.vcf}"""""),
        a.files.tbi -> (tbiId, "application/octet-stream", s""""attachment; filename="${a.files.tbi}"""""),
        a.files.qc -> (qcId, "application/octet-stream", s""""attachment; filename="${a.files.qc}""""")
      )

    }.toMap
    fileEntries
      .flatMap { f =>
        mapOfIds.get(f.filename).map {
          case (id, contentType, contentDisposition) => FileEntry(f, id, contentType, contentDisposition)
        }
      }

  }


  def revert(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: S3Client): Unit = {
    println("################# Reverting Copy Files ##################")
    files.foreach { f =>
      val del = DeleteObjectRequest.builder().bucket(bucketDest).key(s"$pathDest/${f.id}").build()
      s3Client.deleteObject(del)
    }
  }

  def copyFiles(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: S3Client): Unit = {
    println("################# Copy Files ##################")
    files.foreach { f =>
      val encodedUrl = URLEncoder.encode(f.bucket + "/" + f.key, StandardCharsets.UTF_8.toString)
      val cp = CopyObjectRequest.builder()
        .copySource(encodedUrl)
        .contentType(f.contentType)
        .contentDisposition(f.contentDisposition)
        .destinationBucket(bucketDest)
        .destinationKey(s"$pathDest/${f.id}")
        .metadataDirective(MetadataDirective.REPLACE)
        .build()

      s3Client.copyObject(cp)
    }
  }

}
