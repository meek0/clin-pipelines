package task

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectListing
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._
import scala.annotation.tailrec

object CheckS3DataTask {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  val requiredFiles: Set[String] = Set(".cram", ".crai", ".tsv", ".gvcf", ".vcf", ".json")

  def run(bucket: String, s3Client: AmazonS3): List[String] = nextBatch(s3Client, s3Client.listObjects(bucket))

  def run(bucket: String, prefix: String, s3Client: AmazonS3): List[String] = nextBatch(s3Client, s3Client.listObjects(bucket, prefix))

  @tailrec
  private def nextBatch(s3Client: AmazonS3, listing: ObjectListing, keys: List[String] = Nil): List[String] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(_.getKey).toList

    if (listing.isTruncated) {
      nextBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: keys)
    } else
      pageKeys ::: keys
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

      val missingFiles: Set[String] = requiredFiles.filterNot(fileExtensions.contains)

      if (missingFiles.size > 0) {
        LOGGER.error(s"The following required files are missing from the batch : ${missingFiles.mkString(",")}")
        false
      } else {
        true
      }
    }
  }
}
