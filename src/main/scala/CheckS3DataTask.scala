import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectListing

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CheckS3DataTask {

  def run(bucket: String, s3Client: AmazonS3): List[String] = nextBatch(s3Client, s3Client.listObjects(bucket))

  @tailrec
  private def nextBatch(s3Client: AmazonS3, listing: ObjectListing, keys: List[String] = Nil): List[String] = {
    val pageKeys = listing.getObjectSummaries.asScala.map(_.getKey).toList

    if (listing.isTruncated) {
      nextBatch(s3Client, s3Client.listNextBatchOfObjects(listing), pageKeys ::: keys)
    } else
      pageKeys ::: keys
  }
}
