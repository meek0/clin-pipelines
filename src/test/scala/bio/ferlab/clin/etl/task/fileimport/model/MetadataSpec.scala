package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.testutils.MinioServerSuite
import org.scalatest.{FlatSpec, Matchers}

class MetadataSpec extends FlatSpec with MinioServerSuite with Matchers {

  "validateMetadata" should "return a SimpleMetadata instance" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(prefix, "good")

      val meta = Metadata.validateMetadataFile(inputBucket, prefix)
      println(meta)
      meta.isValid shouldBe true
    }

  }
  it should "return a FullMetadata instance" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(prefix, "full")

      val meta = Metadata.validateMetadataFile(inputBucket, prefix, full = true)
      println(meta)
      meta.isValid shouldBe true
    }

  }


}