package bio.ferlab.clin.etl.s3

import bio.ferlab.clin.etl.testutils.MinioServerSuite
import cats.data.NonEmptyList
import cats.data.Validated.Invalid
import cats.implicits.catsSyntaxValidatedId
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.collection.JavaConverters._

class S3UtilsSpec extends FlatSpec with MinioServerSuite with Matchers {

  "writeContent" should "write file content in s3" in {
    withS3Objects { (prefix, _) =>
      val path = s"$prefix/hello.txt"
      val content = "hello world"
      S3Utils.writeContent(inputBucket, path, content)

      val result = S3Utils.getContent(inputBucket, path)
      result shouldBe content

    }

  }

  "getContent" should "get file content in a string" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(prefix, "s3test")

      val result = S3Utils.getContent(inputBucket, s"$prefix/hello.txt")
      result shouldBe "hello world"

    }

  }

  "exists" should "return false if file does not exist" in {
    withS3Objects { (_, _) =>
      S3Utils.exists(inputBucket, "folder/does_not_exist.txt") shouldBe false


    }

  }

  it should "return true if file does not exist" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(prefix, "s3test")
      S3Utils.exists(inputBucket, s"$prefix/hello.txt") shouldBe true


    }

  }
}
