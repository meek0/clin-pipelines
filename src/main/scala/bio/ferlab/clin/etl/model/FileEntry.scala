package bio.ferlab.clin.etl.model

import com.amazonaws.services.s3.AmazonS3

import java.util.UUID

case class FileEntry(bucket: String, key: String, md5: String, size: Long) {
  val id: String = UUID.randomUUID().toString
  lazy val filename: String = key.substring(key.lastIndexOf("/") + 1)
}

