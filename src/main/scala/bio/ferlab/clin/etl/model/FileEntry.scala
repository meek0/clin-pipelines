package bio.ferlab.clin.etl.model

case class FileEntry(
                      bucket: String,
                      key: String,
                      md5: String,
                      size: Long,
                      id: String) {
  lazy val filename: String = FileEntry.getFileName(key)
}

object FileEntry {
  def apply(raw: RawFileEntry, id: String): FileEntry = {
    new FileEntry(raw.bucket, raw.key, raw.md5, raw.size, id)
  }

  def getFileName(key:String): String = key.substring(key.lastIndexOf("/") + 1)
}

case class RawFileEntry(bucket: String, key: String, md5: String, size: Long) {
  lazy val filename: String = FileEntry.getFileName(key)
}

