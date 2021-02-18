package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MinioServerSuite
import bio.ferlab.clin.etl.model.FileEntry
import bio.ferlab.clin.etl.task.CheckS3Data
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.scalatest.{FlatSpec, Matchers}

import java.io.File

class CheckS3DataSpec extends FlatSpec with MinioServerSuite with Matchers {

  "loadFileEntries" should "return list of files present in bucket" in {
    withBuckets { (inputBucket, _) =>
      val transferManager = TransferManagerBuilder.standard.withS3Client(s3).build()
      val prefix = "run_abc"
      val transfert = transferManager.uploadDirectory(inputBucket, prefix, new File(getClass.getResource("/good").toURI), false);
      transfert.waitForCompletion()

      val fileEntries = CheckS3Data.loadFileEntries(inputBucket, "run_abc")

      fileEntries should contain theSameElementsAs List(
        FileEntry(inputBucket, s"$prefix/file1.crai", "8e2c4493b1fb0f04c6a8a9e393216ae8", 10),
        FileEntry(inputBucket, s"$prefix/file1.cram", "3aea99c07f21c8cd0207964d0b25dc47", 10),
        FileEntry(inputBucket, s"$prefix/file2.tbi", "3f6f141d2b7a40cddd914ea4d0fe89fa", 9),
        FileEntry(inputBucket, s"$prefix/file2.vcf", "ddad696feb7fd44bd8e0de271aa3c291", 9),
        FileEntry(inputBucket, s"$prefix/file3.json", "d1b484d132ddfd1774481044bbea07ce", 8))
    }

  }

}
