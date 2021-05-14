package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.isValid
import bio.ferlab.clin.etl.model.{FileEntry, Metadata, RawFileEntry}
import cats.data.ValidatedNel
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectListing
import org.slf4j.{Logger, LoggerFactory}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CheckS3Data {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def ls(bucket: String, prefix: String)(implicit s3Client: AmazonS3): List[RawFileEntry] = nextBatch(s3Client, s3Client.listObjects(bucket, prefix))

  @tailrec
  private def nextBatch(s3Client: AmazonS3, listing: ObjectListing, objects: List[RawFileEntry] = Nil): List[RawFileEntry] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(o => RawFileEntry(o.getBucketName, o.getKey, o.getETag, o.getSize)).toList

    if (listing.isTruncated) {
      nextBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: objects)
    } else
      pageKeys ::: objects
  }

  def validateFileEntries(rawFileEntries: Seq[RawFileEntry], fileEntries: Seq[FileEntry]): ValidatedNel[String, Seq[FileEntry]] = {
    println("################# Validate File entries ##################")
    val fileEntriesNotInAnalysis = rawFileEntries.filterNot(r => fileEntries.exists(f => f.key == r.key))
    val errorFilesNotExist = fileEntriesNotInAnalysis.map(f => s"File ${f.filename} not found in metadata JSON file.")
    isValid(fileEntries, errorFilesNotExist)
  }

  def loadRawFileEntries(bucket: String, prefix: String)(implicit s3CLient: AmazonS3): Seq[RawFileEntry] = {
    val fileEntries = ls(bucket, prefix)
      .filter(f => f.filename != "_SUCCESS" && f.filename != "metadata.json" && !f.filename.toLowerCase().startsWith("combined_vcf"))
    fileEntries
  }


  def loadFileEntries(m: Metadata, fileEntries: Seq[RawFileEntry], generateId: () => String = () => UUID.randomUUID().toString): Seq[FileEntry] = {
    val mapOfIds = m.analyses.flatMap { a =>
      val cramId: String = generateId()
      val vcfId: String = generateId()
      val qcId: String = generateId()

      Seq(
        a.files.cram -> cramId, a.files.crai -> s"$cramId.crai",
        a.files.vcf -> vcfId, a.files.tbi -> s"$vcfId.tbi",
        a.files.qc -> qcId
      )

    }.toMap
    fileEntries.flatMap { f => mapOfIds.get(f.filename).map(id => FileEntry(f, id)) }

  }


  def revert(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: AmazonS3): Unit = {
    println("################# Reverting Copy Files ##################")
    files.foreach { f =>
      s3Client.deleteObject(bucketDest, s"$pathDest/${f.id}")
    }
  }

  def copyFiles(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: AmazonS3): Unit = {
    println("################# Copy Files ##################")
    files.foreach { f =>
      s3Client.copyObject(f.bucket, f.key, bucketDest, s"$pathDest/${f.id}")
    }
  }

}
