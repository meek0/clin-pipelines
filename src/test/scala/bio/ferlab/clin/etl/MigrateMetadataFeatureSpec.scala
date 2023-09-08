package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.scripts.MigrateMetadata
import bio.ferlab.clin.etl.task.fileimport.model._
import bio.ferlab.clin.etl.testutils.MinioServer
import org.scalatest.{FlatSpec, Matchers}

class MigrateMetadataFeatureSpec extends FlatSpec with MinioServer with Matchers {

  "run" should "return no errors" in {
    withS3Objects { (inputPrefix, _) =>

      transferFromResources(inputPrefix, "old")
      val result = MigrateMetadata.run(inputBucket, inputPrefix)
      result.isValid shouldBe true
      val newFileResult = Metadata.validateMetadataFile(inputBucket,inputPrefix, full=true)
      newFileResult.isValid shouldBe true
      val m: Metadata = newFileResult.toEither.right.get
      m.analyses.size shouldBe 1
      m.analyses.head shouldBe FullAnalysis("_LDM_ORGANIZATION_ID_",
        "_LDM_SAMPLE_ID_","_LDM_SPECIMEN_ID_",
        "NBL",Some("NBL"),"_LDM_SERVICE_REQUEST_ID_","nanuq_sample_id",
        FullPatient("John","Doe","male",Some("_RAMQ_"),"06/03/2001",Some("_MRN_"),"_EP_ORGANIZATION_ID_","SOLO","PROB",None,"AFF",None),
        FilesAnalysis("file1.cram","file1.crai","file2.vcf","file2.tbi","file4.vcf","file4.tbi",Some("file5.vcf"),Some("file5.tbi"),"file3.json",Some("file6.html"),Some("file6.json"),Some("file6.variants.tsv"),Some("file7.seg.bw"),Some("file7.baf.bw"),Some("file7.roh.bed"),Some("file7.exome.bed"),Some("file8.png"),Some("file9.csv"),Some("file10.json"),Some("file11.tsv")),
        Some("MMG"), None,
        Experiment(Some("Illumina"),Some("NB552318"),Some("runNameExample"),Some("2014-09-21T11:50:23-05:00"),Some("runAliasExample"),Some("0"),Some(true),Some(100),Some("WXS"),Some("RocheKapaHyperExome"),Some("KAPA_HyperExome_hg38_capture_targets")),
        Workflow(Some("Dragen"),Some("1.1.0"),Some("GRCh38"))
      )


    }

  }
}