package bio.ferlab.clin.etl.model

import com.amazonaws.services.s3.AmazonS3

import java.util.UUID

case class FileEntry(bucket: String, key: String, md5: String, size: Long, id: String = UUID.randomUUID().toString) {
  lazy val filename: String = key.substring(key.lastIndexOf("/") + 1)
}

object FileEntry {
  def revert(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: AmazonS3): Unit = files.foreach { f =>
    s3Client.deleteObject(bucketDest, s"$pathDest/${f.id}")
  }

  def copyFiles(files: Seq[FileEntry], bucketDest: String, pathDest: String)(implicit s3Client: AmazonS3): Unit = {
    files.foreach { f =>
      s3Client.copyObject(f.bucket, f.key, bucketDest, s"$pathDest/${f.id}")
    }
  }
}
