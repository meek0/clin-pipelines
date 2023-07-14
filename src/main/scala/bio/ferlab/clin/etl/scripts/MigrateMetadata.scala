package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.task.fileimport.model.OldMetadata
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits.catsSyntaxValidatedId
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json
import software.amazon.awssdk.services.s3.S3Client

object MigrateMetadata {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def apply(prefix: String, conf: Conf): ValidatedNel[String, String] = {
    implicit val s3Client: S3Client = buildS3Client(conf.aws)
    val bucket = conf.aws.bucketName
    run(bucket, prefix)
  }

  def run(bucket: String, prefix: String)(implicit s3Client: S3Client): ValidationResult[String] = {
    val oldMetadata: ValidatedNel[String, OldMetadata] = OldMetadata.validateMetadataFile(bucket, prefix)
    oldMetadata.andThen { m: OldMetadata =>
      LOGGER.info("Old metadata is valid, convert it to new metadata")
      val newMetadata = m.toNewMetadata
      val json = Json.prettyPrint(Json.toJson(newMetadata))
      val s3Path = s"$prefix/metadata.json"
      LOGGER.info(s"Write content into bucker $bucket at path $s3Path with content: $json")
      S3Utils.writeContent(bucket, s3Path, json)
      json.validNel[String]
    }
  }


}
