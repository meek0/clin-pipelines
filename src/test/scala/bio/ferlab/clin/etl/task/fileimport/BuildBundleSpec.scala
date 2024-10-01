package bio.ferlab.clin.etl.task.fileimport

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, TBundle}
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.{defaultAnalysis, defaultAnalysisWithOptionals, defaultMetadata, defaultMetadataWithOptionals, defaultPatient, extumMetadataInvalidQcMetrics, extumMetadataWithOptionals, unsupportedMetadataWithOptionals}
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
      FileEntry("bucket", "file1.cram", Some("md5"), 10, "1.cram", "application/octet-stream", ""),
      FileEntry("bucket", "file1.cram.crai", Some("md5"), 10, "1.cram.crai", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf.gz.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf.gz.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file5.vcf.gz", Some("md5"), 10, "3.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file5.vcf.gz.tbi", Some("md5"), 10, "3.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file3.tgz", Some("md5"), 10, "4.tgz", "application/octet-stream", ""),
      FileEntry("bucket", "file6.html", Some("md5"), 10, "4.html", "application/octet-stream", ""),
      FileEntry("bucket", "file6.json", Some("md5"), 10, "4.json", "application/octet-stream", ""),
      FileEntry("bucket", "file6.variants.tsv", Some("md5"), 10, "4.variants.tsv", "application/octet-stream", ""),
      FileEntry("bucket", "file7.seg.bw", Some("md5"), 10, "4.seg.bw", "application/octet-stream", ""),
      FileEntry("bucket", "file7.baf.bw", Some("md5"), 10, "4.baf.bw", "application/octet-stream", ""),
      FileEntry("bucket", "file7.roh.bed", Some("md5"), 10, "4.roh.bed", "application/octet-stream", ""),
      FileEntry("bucket", "file7.exome.bed", Some("md5"), 10, "4.exome.bed", "application/octet-stream", ""),
      FileEntry("bucket", "file8.png", Some("md5"), 10, "4.png", "application/octet-stream", ""),
      FileEntry("bucket", "file9.csv", Some("md5"), 10, "4.csv", "application/octet-stream", ""),
      FileEntry("bucket", "file10.json", Some("md5"), 10, "4.json", "application/octet-stream", ""),
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files, "BAT1")
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
      FileEntry("bucket", "file1.cram", Some("md5"), 10, "1.cram", "application/octet-stream", ""),
      FileEntry("bucket", "file1.cram.crai", Some("md5"), 10, "1.cram.crai", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf.gz.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf.gz.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file5.vcf.gz", Some("md5"), 10, "3.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file5.vcf.gz.tbi", Some("md5"), 10, "3.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file3.tgz", Some("md5"), 10, "4.tgz", "application/octet-stream", "")
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files, "BAT1")
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
      FileEntry("bucket", "file1.cram", Some("md5"), 10, "1.cram", "application/octet-stream", ""),
      FileEntry("bucket", "file1.cram.crai", Some("md5"), 10, "1.cram.crai", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf.gz.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf.gz.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      //FileEntry("bucket", "file5.vcf.gz", Some("md5"), 10, "3.vcf.gz", "application/octet-stream", ""),
      //FileEntry("bucket", "file5.vcf.gz.tbi", Some("md5"), 10, "3.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file3.tgz", Some("md5"), 10, "4.tgz", "application/octet-stream", "")
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files, "BAT1")
    println(result)
    result.isValid shouldBe false
    val error = result match {
      case Invalid(NonEmptyList(h, _)) => h
      case Valid(_) => ""
    }
    assert(error.equals("File file5.vcf.gz does not exist : type=vcf, specimen=submitted_specimen_id, sample=submitted_sample_id"))
  }

  "it" should "Failed (Unsupported metadata)" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val organizationAlias = nextId()
    FhirTestUtils.loadOrganizations(organizationAlias)
    val serviceRequestId = FhirTestUtils.loadServiceRequest(ptId)
    val meta = unsupportedMetadataWithOptionals.copy(analyses = Seq(
      defaultAnalysisWithOptionals.copy(patient = defaultPatient(ptId), clinServiceRequestId = serviceRequestId, ldm = organizationAlias)
    ))
    val files = Seq(
      FileEntry("bucket", "file1.cram", Some("md5"), 10, "1.cram", "application/octet-stream", ""),
      FileEntry("bucket", "file1.crai", Some("md5"), 10, "1.cram.crai", "application/octet-stream", ""),
      FileEntry("bucket", "file2.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file2.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file4.vcf.gz", Some("md5"), 10, "2.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file4.tbi", Some("md5"), 10, "2.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file5.vcf.gz", Some("md5"), 10, "3.vcf.gz", "application/octet-stream", ""),
      FileEntry("bucket", "file5.tbi", Some("md5"), 10, "3.vcf.gz.tbi", "application/octet-stream", ""),
      FileEntry("bucket", "file3.tgz", Some("md5"), 10, "4.tgz", "application/octet-stream", "")
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files, "BAT1")
    println(result)
    result.isValid shouldBe false
    val error = result match {
      case Invalid(NonEmptyList(h, _)) => h
      case Valid(_) => ""
    }
    assert(error.equals("Unsupported metadata schema, should be one of: [CQGC_Germline, CQGC_Exome_Tumeur_Seul]"))
  }

}