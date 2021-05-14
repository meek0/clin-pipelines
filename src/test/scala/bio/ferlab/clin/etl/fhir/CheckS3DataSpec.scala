package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._
import bio.ferlab.clin.etl.fhir.testutils.MinioServerSuite
import bio.ferlab.clin.etl.model.{FileEntry, RawFileEntry}
import bio.ferlab.clin.etl.task.CheckS3Data
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.{FlatSpec, Matchers}

class CheckS3DataSpec extends FlatSpec with MinioServerSuite with Matchers {

  "loadFileEntries" should "return list of files present in bucket" in {
    withObjects { (prefix, _) =>
      transferFromResources(prefix, "good")

      val fileEntries = CheckS3Data.loadRawFileEntries(inputBucket, prefix)

      fileEntries should contain theSameElementsAs List(
        RawFileEntry(inputBucket, s"$prefix/file1.crai", "8e2c4493b1fb0f04c6a8a9e393216ae8", 10),
        RawFileEntry(inputBucket, s"$prefix/file1.cram", "3aea99c07f21c8cd0207964d0b25dc47", 10),
        RawFileEntry(inputBucket, s"$prefix/file2.tbi", "3f6f141d2b7a40cddd914ea4d0fe89fa", 9),
        RawFileEntry(inputBucket, s"$prefix/file2.vcf", "ddad696feb7fd44bd8e0de271aa3c291", 9),
        RawFileEntry(inputBucket, s"$prefix/file3.json", "d1b484d132ddfd1774481044bbea07ce", 8))
    }

  }

  private def fileEntry(key: String, id: String) = FileEntry(inputBucket, key, "md5", 1, id)

  private def rawFileEntry(key: String) = RawFileEntry(inputBucket, key, "md5", 1)

  val files = Seq(
    fileEntry(s"file1.cram", "abc"),
    fileEntry(s"file1.crai", "def"),
    fileEntry(s"file2.vcf", "ghi"),
    fileEntry(s"file2.tbi", "jkl"),
    fileEntry(s"file3.tgz", "mno")
  )

  "copyFiles" should "move files from one bucket to the other" in {
    withObjects { (inputPrefix, outputPrefix) =>
      transferFromResources(inputPrefix, "good")
      val files = Seq(
        fileEntry(s"$inputPrefix/file1.cram", "abc"),
        fileEntry(s"$inputPrefix/file1.crai", "def"),
        fileEntry(s"$inputPrefix/file2.vcf", "ghi")
      )
      CheckS3Data.copyFiles(files, outputBucket, outputPrefix)
      list(outputBucket, outputPrefix) should contain theSameElementsAs Seq(s"$outputPrefix/abc", s"$outputPrefix/def", s"$outputPrefix/ghi")
    }
  }

  "revert" should "move back files from one bucket to the other" in {
    withObjects { (inputPrefix, outputPrefix) =>
      transferFromResources(outputPrefix, "revert", outputBucket)
      val files = Seq(
        fileEntry(s"$inputPrefix/file1.cram", "file1"),
        fileEntry(s"$inputPrefix/file1.crai", "file2"),
        fileEntry(s"$inputPrefix/file2.vcf", "file3")
      )
      CheckS3Data.revert(files, outputBucket, outputPrefix)
      list(outputBucket, outputPrefix) shouldBe empty
    }
  }

  val rawFiles = Seq(
    rawFileEntry(s"file1.cram"),
    rawFileEntry(s"file1.crai"),
    rawFileEntry(s"file2.vcf"),
    rawFileEntry(s"file2.tbi"),
    rawFileEntry(s"file3.tgz")
  )
  "validateFiles" should "return errors if input bucket contains files that are not present into metadata" in {

    val badRawFiles = rawFiles ++ Seq(rawFileEntry(s"file_not_in_metadata.cram"),
      rawFileEntry(s"file_not_in_metadata2.cram"))
    val result = CheckS3Data.validateFileEntries(badRawFiles, files)

    result shouldBe Invalid(NonEmptyList.of(
      "File file_not_in_metadata.cram not found in metadata JSON file.", "File file_not_in_metadata2.cram not found in metadata JSON file."
    ))


  }

  "validateFiles" should "return list of files if input bucket contains files that are not present into metadata" in {
    val result = CheckS3Data.validateFileEntries(rawFiles, files)

    result shouldBe Valid(files)
  }

  "loadFileEntries" should "return file entries based on raw data with id inferred for cram/crai and vcf/tbi and qc" in {
    var i = 0;
    val generatorId = () => {
      i=i+1
      s"id_$i"
    }
    CheckS3Data.loadFileEntries(defaultMetadata, rawFiles, generatorId) shouldBe Seq(
      fileEntry(s"file1.cram", "id_1"),
      fileEntry(s"file1.crai", "id_1.crai"),
      fileEntry(s"file2.vcf", "id_2"),
      fileEntry(s"file2.tbi", "id_2.tbi"),
      fileEntry(s"file3.tgz", "id_3")
    )
  }

  it should "return file entries based and should ignore files that exist in metadata but not exist in objectstore" in {
    val inputRawFiles = Seq(rawFileEntry(s"file3.tgz"))
    CheckS3Data.loadFileEntries(defaultMetadata, inputRawFiles, () => "id") shouldBe Seq(
      fileEntry(s"file3.tgz", "id")
    )
  }

  it should "return file entries based and should ignore files that exist in object store but not exist in metadata" in {
    val inputRawFiles = rawFiles ++ Seq(rawFileEntry(s"file_not_in_metadata.cram"))
    CheckS3Data.loadFileEntries(defaultMetadata, inputRawFiles, () => "id") shouldBe Seq(
      fileEntry(s"file1.cram", "id"),
      fileEntry(s"file1.crai", "id.crai"),
      fileEntry(s"file2.vcf", "id"),
      fileEntry(s"file2.tbi", "id.tbi"),
      fileEntry(s"file3.tgz", "id")
    )
  }
}
