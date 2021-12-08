package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.testutils.MinioServerSuite
import bio.ferlab.clin.etl.s3.S3Utils
import cats.data.NonEmptyList
import cats.data.Validated.Invalid
import cats.implicits.{catsSyntaxValidatedId, toFlatMapOps}
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.collection.JavaConverters._

class ETLSpec extends FlatSpec with MinioServerSuite with Matchers {
  "withReport" should "write errors in an output bucket" in {
    withS3Objects { (prefix, _) =>

      withReport(inputBucket, prefix) { _ =>
        Invalid(NonEmptyList("error1", List("error2")))
      }
      val ls = s3.listObjects(ListObjectsRequest.builder().bucket(inputBucket).prefix(prefix).build()).contents().asScala.map(_.key())
      ls.size shouldBe 1
      ls.head should endWith("error.txt")

      val errorReport = S3Utils.getContent(inputBucket, ls.head)
      errorReport shouldBe "error1\nerror2"
    }

  }
  it should "write exception in an output bucket" in {
    withS3Objects { (prefix, _) =>

      try {
        withReport(inputBucket, prefix) { _ =>
          throw new IllegalStateException("error1")
        }
      } catch {
        case _: Exception =>
      }
      val ls = s3.listObjects(ListObjectsRequest.builder().bucket(inputBucket).prefix(prefix).build()).contents().asScala.map(_.key())
      ls.size shouldBe 1
      ls.head should endWith("error.txt")

      val errorReport = S3Utils.getContent(inputBucket, ls.head)
      errorReport.contains("java.lang.IllegalStateException: error1") shouldBe true
    }

  }

  it should "write success.txt if success" in {
    withS3Objects { (prefix, _) =>

      withReport(inputBucket, prefix) { _ =>
        "success".validNel[String]
      }
      val ls = s3.listObjects(ListObjectsRequest.builder().bucket(inputBucket).prefix(prefix).build()).contents().asScala.map(_.key())
      ls.size shouldBe 1
      ls.head should endWith("success.txt")

    }

  }

  "flatMap" should "flatten validation results if valid" in {
    val task: ValidationResult[String] = "valid task".validNel[String]
    task.flatMap(t => "error".invalidNel[String]) shouldBe "error".invalidNel[String]
    task.flatMap(t => "valid result".validNel[String]) shouldBe "valid result".validNel[String]
  }

  "flatMap" should "flatten validation results if invalid" in {
    val task: ValidationResult[String] = "invalid task".invalidNel[String]
    task.flatMap(t => "error".invalidNel[String]) shouldBe "invalid task".invalidNel[String]
    task.flatMap(t => "valid result".validNel[String]) shouldBe "invalid task".invalidNel[String]
  }
}