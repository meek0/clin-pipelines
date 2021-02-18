package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.isValid
import bio.ferlab.clin.etl.model.{FileEntry, Metadata}
import cats.data.ValidatedNel
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectListing
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CheckS3Data {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def ls(bucket: String, prefix: String)(implicit s3Client: AmazonS3): List[FileEntry] = nextBatch(s3Client, s3Client.listObjects(bucket, prefix))

  @tailrec
  private def nextBatch(s3Client: AmazonS3, listing: ObjectListing, objects: List[FileEntry] = Nil): List[FileEntry] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(o => FileEntry(o.getBucketName, o.getKey, o.getETag, o.getSize)).toList

    if (listing.isTruncated) {
      nextBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: objects)
    } else
      pageKeys ::: objects
  }

  def validateFileEntries(m: Metadata, fileEntries: Seq[FileEntry]): ValidatedNel[String, Seq[FileEntry]] = {
    val filesFromAnalysis = m.analyses.flatMap(a => Seq(a.files.cram, a.files.crai, a.files.vcf, a.files.tbi, a.files.qc))
    val fileEntriesNotInAnalysis = fileEntries.filterNot(f => filesFromAnalysis.contains(f.filename))

    val fileEntriesExistInMetadata = fileEntriesNotInAnalysis.map(f => s"File ${f.filename} not found in metadata JSON file.")
    isValid(fileEntries, fileEntriesExistInMetadata)
  }

  def loadFileEntries(bucket: String, prefix: String)(implicit s3CLient: AmazonS3): Seq[FileEntry] = {
    val fileEntries = ls(bucket, prefix)
      .filter(f => f.filename != "_SUCCESS" && f.filename != "metadata.json" && !f.filename.toLowerCase().startsWith("combined_vcf"))
    fileEntries
  }

  def revert(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: AmazonS3): Unit = files.foreach { f =>
    s3Client.deleteObject(bucketDest, s"$pathDest/${f.id}")
  }

  def copyFiles(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: AmazonS3): Unit = {
    files.foreach { f =>
      s3Client.copyObject(f.bucket, f.key, bucketDest, s"$pathDest/${f.id}")
    }
  }

}
