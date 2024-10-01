package bio.ferlab.clin.etl.task.fileimport

import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, RawFileEntry}
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.defaultMetadata
import bio.ferlab.clin.etl.testutils.MinioServerSuite
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.{FlatSpec, Matchers}

class CheckS3DataSpec extends FlatSpec with MinioServerSuite with Matchers {

  "loadFileEntries" should "return list of files present in bucket" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(prefix, "good")

      val fileEntries = CheckS3Data.loadRawFileEntries(inputBucket, prefix)

      val expected = List(
        RawFileEntry(inputBucket, s"$prefix/file1.cram.crai", 10),
        RawFileEntry(inputBucket, s"$prefix/file1.cram", 10),
        RawFileEntry(inputBucket, s"$prefix/file1.cram.md5sum", 13),
        RawFileEntry(inputBucket, s"$prefix/file2.vcf.gz.tbi", 9),
        RawFileEntry(inputBucket, s"$prefix/file2.vcf.gz", 9),
        RawFileEntry(inputBucket, s"$prefix/file2.vcf.gz.md5sum", 12),
        RawFileEntry(inputBucket, s"$prefix/file4.vcf.gz.tbi", 9),
        RawFileEntry(inputBucket, s"$prefix/file4.vcf.gz", 9),
        RawFileEntry(inputBucket, s"$prefix/file4.vcf.gz.md5sum", 12),
        RawFileEntry(inputBucket, s"$prefix/file5.vcf.gz.tbi", 9),
        RawFileEntry(inputBucket, s"$prefix/file5.vcf.gz", 9),
        RawFileEntry(inputBucket, s"$prefix/file5.vcf.gz.md5sum", 12),
        RawFileEntry(inputBucket, s"$prefix/file3.json", 8),
        RawFileEntry(inputBucket, s"$prefix/file6.html", 19),
        RawFileEntry(inputBucket, s"$prefix/file6.json", 19),
        RawFileEntry(inputBucket, s"$prefix/file6.variants.tsv", 18),
        RawFileEntry(inputBucket, s"$prefix/file7.seg.bw", 22),
        RawFileEntry(inputBucket, s"$prefix/file7.baf.bw", 22),
        RawFileEntry(inputBucket, s"$prefix/file7.roh.bed", 23),
        RawFileEntry(inputBucket, s"$prefix/file7.exome.bed", 26),
        RawFileEntry(inputBucket, s"$prefix/file8.png", 22),
        RawFileEntry(inputBucket, s"$prefix/file9.csv", 26),
        RawFileEntry(inputBucket, s"$prefix/file10.json", 20),
      )
      fileEntries should contain theSameElementsAs expected
    }

  }

  "ls" should "return  of giles even if s3 listing is truncated" in {
    withS3Objects { (prefix, _) =>
      copyNFile(prefix, "good/file1.cram", 30)
      val fileEntries = CheckS3Data.ls(inputBucket, prefix, 10)

      fileEntries.size shouldBe 30

    }
  }

  private def fileEntry(key: String, id: String, filename: String, md5: Option[String] = None) = FileEntry(inputBucket, key, md5, 1, id, "application/octet-stream", s"""attachment; filename="$filename"""")

  private def rawFileEntry(key: String) = RawFileEntry(inputBucket, key, 1)

  val files: Seq[FileEntry] = Seq(
    fileEntry(s"file1.cram", "abc", "file1.cram"),
    fileEntry(s"file1.cram.crai", "def", "file1.cram.crai"),
    fileEntry(s"file2.vcf.gz", "ghi", "file2.vcf.gz"),
    fileEntry(s"file2.vcf.gz.tbi", "jkl", "file2.vcf.gz.tbi"),
    fileEntry(s"file3.tgz", "mno", "file3.tgz")
  )

  "copyFiles" should "move files from one bucket to the other" in {
    withS3Objects { (inputPrefix, outputPrefix) =>
      transferFromResources(inputPrefix, "good")
      val files = Seq(
        fileEntry(s"$inputPrefix/file1.cram", s"$outputPrefix/abc.cram", "file1.cram"),
        fileEntry(s"$inputPrefix/file1.cram.crai", s"$outputPrefix/def.cram.crai", "file1.cram.crai"),
        fileEntry(s"$inputPrefix/file2.vcf.gz", s"$outputPrefix/ghi.vcf.gz", "file2.vcf.gz")
      )
      CheckS3Data.copyFiles(files, outputBucket)
      list(outputBucket, outputPrefix) should contain theSameElementsAs Seq(s"$outputPrefix/abc.cram", s"$outputPrefix/def.cram.crai", s"$outputPrefix/ghi.vcf.gz")
    }
  }

  "revert" should "move back files from one bucket to the other" in {
    withS3Objects { (inputPrefix, outputPrefix) =>
      transferFromResources(outputPrefix, "revert", outputBucket)
      val files = Seq(
        fileEntry(s"$inputPrefix/file1.cram", s"$outputPrefix/file1.cram", "file1.cram"),
        fileEntry(s"$inputPrefix/file1.cram.crai", s"$outputPrefix/file2.cram.crai", "file1.cram.crai"),
        fileEntry(s"$inputPrefix/file2.vcf.gz", s"$outputPrefix/file3.vcf.gz", "file2.vcf.gz")
      )
      CheckS3Data.revert(files, outputBucket)
      list(outputBucket, outputPrefix) shouldBe empty
    }
  }

  val rawFiles: Seq[RawFileEntry] = Seq(
    rawFileEntry(s"file1.cram"),
    rawFileEntry(s"file1.cram.md5sum"),
    rawFileEntry(s"file1.cram.crai"),
    rawFileEntry(s"file2.vcf.gz"),
    rawFileEntry(s"file1.vcf.gz.md5sum"),
    rawFileEntry(s"file2.vcf.gz.tbi"),
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

  it should "return list of files if input bucket contains files that are not present into metadata" in {
    val result = CheckS3Data.validateFileEntries(rawFiles, files)

    result shouldBe Valid(files)
  }

  "loadFileEntries" should "return file entries based on raw data" in {
    withS3Objects { (inputPrefix, outputPrefix) =>
      transferFromResources(inputPrefix, "good")
      var i = 0
      val generatorId = () => {
        i = i + 1
        s"id_$i"
      }
      val rawFiles = Seq(
        rawFileEntry(s"$inputPrefix/file1.cram"),
        rawFileEntry(s"$inputPrefix/file1.cram.md5sum"),
        rawFileEntry(s"$inputPrefix/file1.cram.crai"),
        rawFileEntry(s"$inputPrefix/file2.vcf.gz"),
        rawFileEntry(s"$inputPrefix/file2.vcf.gz.md5sum"),
        rawFileEntry(s"$inputPrefix/file2.vcf.gz.tbi"),
        rawFileEntry(s"$inputPrefix/file4.vcf.gz"),
        rawFileEntry(s"$inputPrefix/file4.vcf.gz.md5sum"),
        rawFileEntry(s"$inputPrefix/file4.vcf.gz.tbi"),
        rawFileEntry(s"$inputPrefix/file5.vcf.gz"),
        rawFileEntry(s"$inputPrefix/file5.vcf.gz.md5sum"),
        rawFileEntry(s"$inputPrefix/file5.vcf.gz.tbi"),
        rawFileEntry(s"$inputPrefix/file3.tgz")
      )

      CheckS3Data.loadFileEntries(defaultMetadata, rawFiles, outputPrefix, generatorId) shouldBe Seq(
        fileEntry(s"$inputPrefix/file1.cram", s"$outputPrefix/id_1.cram", "file1.cram", Some("md5 cram file")),
        fileEntry(s"$inputPrefix/file1.cram.crai", s"$outputPrefix/id_1.cram.crai", "file1.cram.crai"),
        fileEntry(s"$inputPrefix/file2.vcf.gz", s"$outputPrefix/id_2.vcf.gz", "file2.vcf.gz", Some("md5 vcf file")),
        fileEntry(s"$inputPrefix/file2.vcf.gz.tbi", s"$outputPrefix/id_2.vcf.gz.tbi", "file2.vcf.gz.tbi"),
        fileEntry(s"$inputPrefix/file4.vcf.gz", s"$outputPrefix/id_3.vcf.gz", "file4.vcf.gz", Some("md5 vcf file")),
        fileEntry(s"$inputPrefix/file4.vcf.gz.tbi", s"$outputPrefix/id_3.vcf.gz.tbi", "file4.vcf.gz.tbi"),
        fileEntry(s"$inputPrefix/file5.vcf.gz", s"$outputPrefix/id_4.vcf.gz", "file5.vcf.gz", Some("md5 vcf file")),
        fileEntry(s"$inputPrefix/file5.vcf.gz.tbi", s"$outputPrefix/id_4.vcf.gz.tbi", "file5.vcf.gz.tbi"),
        fileEntry(s"$inputPrefix/file3.tgz", s"$outputPrefix/id_5.tgz", "file3.tgz")
      )
    }
  }

  "idForPrefix" should "return output/id" in {
    CheckS3Data.idForPrefix("output", "hello") shouldBe "output/hello"
  }

  it should "return id" in {
    CheckS3Data.idForPrefix("", "hello") shouldBe "hello"
    CheckS3Data.idForPrefix(null, "hello") shouldBe "hello"
  }

}