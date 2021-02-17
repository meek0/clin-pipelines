package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.isValid
import bio.ferlab.clin.etl.model.{FileEntry, Metadata}
import cats.data.ValidatedNel
import cats.implicits.catsSyntaxValidatedId
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectListing
import org.slf4j.{Logger, LoggerFactory}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CheckS3DataTask {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  val requiredFiles: Set[String] = Set(".cram", ".crai", ".tsv", ".gvcf", ".vcf", ".json")

  def run(bucket: String, s3Client: AmazonS3): List[String] = nextBatch(s3Client, s3Client.listObjects(bucket)).map(_.key)

  def run(bucket: String, prefix: String, s3Client: AmazonS3): List[String] = nextBatch(s3Client, s3Client.listObjects(bucket, prefix)).map(_.key)

  def ls(bucket: String, prefix: String)(implicit s3Client: AmazonS3): List[FileEntry] = nextBatch(s3Client, s3Client.listObjects(bucket, prefix))


  @tailrec
  private def nextBatch(s3Client: AmazonS3, listing: ObjectListing, objects: List[FileEntry] = Nil): List[FileEntry] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(o => FileEntry(o.getBucketName, o.getKey, o.getETag, o.getSize)).toList

    if (listing.isTruncated) {
      nextBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: objects)
    } else
      pageKeys ::: objects
  }

  def validate(m: Metadata, fileEntries: Seq[FileEntry]): ValidatedNel[String, Seq[FileEntry]] = {
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

  def validateFiles(files: List[String]): Boolean = {
    if (files.isEmpty) {
      LOGGER.error("No files found on S3")
      false
    } else {
      LOGGER.info(s"Found ${files.size} files on S3")

      if (LOGGER.isDebugEnabled())
        files.foreach(f => LOGGER.debug(s"\t ${f}"))

      // Check if all the expected files are present in the batch from S3
      val fileExtensions: Set[String] = files.collect {
        case key if key.indexOf('.') > -1 => key.substring(key.lastIndexOf('.'))
      }.toSet

      val missingFiles: Set[String] = requiredFiles.diff(fileExtensions)

      if (missingFiles.nonEmpty) {
        LOGGER.error(s"The following required files are missing from the batch : ${missingFiles.mkString(",")}")
        false
      } else {
        true
      }
    }
  }
}
