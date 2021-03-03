package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
import bio.ferlab.clin.etl.fhir.testutils.{MetadataTestUtils, MinioServerSuite}
import bio.ferlab.clin.etl.model.{FileEntry, FilesAnalysis, Metadata}
import bio.ferlab.clin.etl.task.CheckS3Data
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.scalatest.{FlatSpec, Matchers}

import java.io.File

class CheckS3DataSpec extends FlatSpec with MinioServerSuite with Matchers {

  "loadFileEntries" should "return list of files present in bucket" in {
    withObjects { (prefix, _) =>
      transferFromResources(prefix, "good")

      val fileEntries = CheckS3Data.loadFileEntries(inputBucket, prefix)

      fileEntries should contain theSameElementsAs List(
        FileEntry(inputBucket, s"$prefix/file1.crai", "8e2c4493b1fb0f04c6a8a9e393216ae8", 10),
        FileEntry(inputBucket, s"$prefix/file1.cram", "3aea99c07f21c8cd0207964d0b25dc47", 10),
        FileEntry(inputBucket, s"$prefix/file2.tbi", "3f6f141d2b7a40cddd914ea4d0fe89fa", 9),
        FileEntry(inputBucket, s"$prefix/file2.vcf", "ddad696feb7fd44bd8e0de271aa3c291", 9),
        FileEntry(inputBucket, s"$prefix/file3.json", "d1b484d132ddfd1774481044bbea07ce", 8))
    }

  }

  class FileEntryWithFixedId(override val key: String, override val id: String) extends FileEntry(inputBucket, key, "md5", 1) {}

  "copyFiles" should "move files from one bucket to the other" in {
    withObjects { (inputPrefix, outputPrefix) =>
      transferFromResources(inputPrefix, "good")
      val files = Seq(
        new FileEntryWithFixedId(s"$inputPrefix/file1.cram", "abc"),
        new FileEntryWithFixedId(s"$inputPrefix/file1.crai", "def"),
        new FileEntryWithFixedId(s"$inputPrefix/file2.vcf", "ghi")
      )
      CheckS3Data.copyFiles(files, outputBucket, outputPrefix)
      list(outputBucket, outputPrefix) should contain theSameElementsAs Seq(s"$outputPrefix/abc", s"$outputPrefix/def", s"$outputPrefix/ghi")
    }
  }

  "revert" should "move back files from one bucket to the other" in {
    withObjects { (inputPrefix, outputPrefix) =>
      transferFromResources(outputPrefix, "revert", outputBucket)
      val files = Seq(
        new FileEntryWithFixedId(s"$inputPrefix/file1.cram", "file1"),
        new FileEntryWithFixedId(s"$inputPrefix/file1.crai", "file2"),
        new FileEntryWithFixedId(s"$inputPrefix/file2.vcf", "file3")
      )
      CheckS3Data.revert(files, outputBucket, outputPrefix)
      list(outputBucket, outputPrefix) shouldBe empty
    }
  }

  "validateFiles" should "return errors if input bucket contains files that are not present into metadata" in {

    val files = Seq(
      new FileEntryWithFixedId(s"file1.cram", "abc"),
      new FileEntryWithFixedId(s"file1.crai", "def"),
      new FileEntryWithFixedId(s"file2.vcf", "ghi"),
      new FileEntryWithFixedId(s"file2.tbi", "jkl"),
      new FileEntryWithFixedId(s"file_not_in_metadata.cram", "jkl"),
      new FileEntryWithFixedId(s"file_not_in_metadata2.cram", "jkl")
    )

    val result = CheckS3Data.validateFileEntries(defaultMetadata, files)

    result shouldBe Invalid(NonEmptyList.of(
      "File file_not_in_metadata.cram not found in metadata JSON file.", "File file_not_in_metadata2.cram not found in metadata JSON file."
    ))


  }

  "validateFiles" should "return list of files if input bucket contains files that are not present into metadata" in {

    val files = Seq(
      new FileEntryWithFixedId(s"file1.cram", "abc"),
      new FileEntryWithFixedId(s"file1.crai", "def"),
      new FileEntryWithFixedId(s"file2.vcf", "ghi"),
      new FileEntryWithFixedId(s"file2.tbi", "jkl"),
      new FileEntryWithFixedId(s"file10.cram", "ghi")
    )
    val m = defaultMetadata.copy(
      analyses = Seq(defaultAnalysis, defaultAnalysis.copy(files = defaultFilesAnalysis.copy(cram = "file10.cram")))
    )
    val result = CheckS3Data.validateFileEntries(m, files)

    result shouldBe Valid(files)


  }
}
