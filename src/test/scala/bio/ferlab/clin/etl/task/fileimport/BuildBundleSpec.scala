package bio.ferlab.clin.etl.task.fileimport

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, TBundle}
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.{defaultAnalysis, defaultAnalysisWithOptionals, defaultMetadata, defaultMetadataWithOptionals, defaultPatient}
import bio.ferlab.clin.etl.testutils.{FhirServerSuite, FhirTestUtils}
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class BuildBundleSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {

  "it" should "Build" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val organizationAlias = nextId()
    FhirTestUtils.loadOrganizations(organizationAlias)
    val serviceRequestId = FhirTestUtils.loadServiceRequest(ptId)
    val meta = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId), clinServiceRequestId = serviceRequestId, ldm = organizationAlias)
    ))
    val files = Seq(
      FileEntry("bucket","file1.cram", Some("md5"), 10, "1", "application/octet-stream", ""),
      FileEntry("bucket","file1.crai", Some("md5"), 10, "1.crai", "application/octet-stream", ""),
      FileEntry("bucket","file2.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket","file2.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("bucket","file4.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket","file4.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("bucket","file5.vcf", Some("md5"), 10, "3", "application/octet-stream", ""),
      FileEntry("bucket","file5.tbi", Some("md5"), 10, "3.tbi", "application/octet-stream", ""),
      FileEntry("bucket","file3.tgz", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file6.html", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file6.json", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file6.variants.tsv", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file7.seg.bw", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file7.baf.bw", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file7.roh.bed", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file7.exome.bed", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file8.png", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file9.csv", Some("md5"), 10, "4", "application/octet-stream", ""),
      FileEntry("bucket","file10.json", Some("md5"), 10, "4", "application/octet-stream", ""),
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files)
    println(result)
    result.isValid shouldBe true
  }

  "it" should "Build (with optionals)" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val organizationAlias = nextId()
    FhirTestUtils.loadOrganizations(organizationAlias)
    val serviceRequestId = FhirTestUtils.loadServiceRequest(ptId)
    val meta = defaultMetadataWithOptionals.copy(analyses = Seq(
      defaultAnalysisWithOptionals.copy(patient = defaultPatient(ptId), clinServiceRequestId = serviceRequestId, ldm = organizationAlias)
    ))
    val files = Seq(
      FileEntry("bucket", "file1.cram", Some("md5"), 10, "1", "application/octet-stream", ""),
      FileEntry("bucket", "file1.crai", Some("md5"), 10, "1.crai", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket", "file2.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket", "file4.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file5.vcf", Some("md5"), 10, "3", "application/octet-stream", ""),
      FileEntry("bucket", "file5.tbi", Some("md5"), 10, "3.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file3.tgz", Some("md5"), 10, "4", "application/octet-stream", "")
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files)
    println(result)
    result.isValid shouldBe true
  }

  "it" should "Failed (with optionals file when missing)" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val organizationAlias = nextId()
    FhirTestUtils.loadOrganizations(organizationAlias)
    val serviceRequestId = FhirTestUtils.loadServiceRequest(ptId)
    val meta = defaultMetadataWithOptionals.copy(analyses = Seq(
      defaultAnalysisWithOptionals.copy(patient = defaultPatient(ptId), clinServiceRequestId = serviceRequestId, ldm = organizationAlias)
    ))
    val files = Seq(
      FileEntry("bucket", "file1.cram", Some("md5"), 10, "1", "application/octet-stream", ""),
      FileEntry("bucket", "file1.crai", Some("md5"), 10, "1.crai", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket", "file2.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket", "file4.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      //FileEntry("bucket", "file5.vcf", Some("md5"), 10, "3", "application/octet-stream", ""),
      //FileEntry("bucket", "file5.tbi", Some("md5"), 10, "3.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file3.tgz", Some("md5"), 10, "4", "application/octet-stream", "")
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files)
    println(result)
    result.isValid shouldBe false
    val error = result match {
      case Invalid(NonEmptyList(h, _)) => h
      case Valid(_) => ""
    }
    assert(error.equals("File file5.vcf does not exist : type=vcf, specimen=submitted_specimen_id, sample=submitted_sample_id"))
  }

}