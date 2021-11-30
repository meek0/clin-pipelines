package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.FhirServerSuite
import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils.defaultAnalysis
import bio.ferlab.clin.etl.task.fileimport.model.{CNV_TBI, CNV_VCF, CRAI, CRAM, CopyNumberVariant, FileEntry, QC, QualityControl, SNV_TBI, SNV_VCF, SequencingAlignment, TDocumentReferences, VariantCalling}
import bio.ferlab.clin.etl.task.fileimport.validation.DocumentReferencesValidation
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class DocumentReferencesValidationSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {
  def fileEntry(bucket: String = "bucket", key: String = "key", md5: String = "md5", size: Long = 10, id: String = "id"): FileEntry = FileEntry(bucket, key, Some(md5), size, id, "application/octet-stream", "content-disposition")

  "validateFiles" should "return error if one file not exist in file entries" in {

    val files = Map(
      "file1.cram" -> fileEntry(key = "file1.cram"),
      "file1.crai" -> fileEntry(key = "file1.crai"),
      "file2.vcf" -> fileEntry(key = "file2.vcf"),
      "file4.vcf" -> fileEntry(key = "file4.vcf"),
      "file4.tbi" -> fileEntry(key = "file4.tbi")
    )
    DocumentReferencesValidation.validateFiles(files, defaultAnalysis) shouldBe Invalid(
      NonEmptyList.of(
        s"File file2.tbi does not exist : type=tbi, specimen=submitted_specimen_id, sample=submitted_sample_id, patient:clin_id",
        "File file3.tgz does not exist : type=qc, specimen=submitted_specimen_id, sample=submitted_sample_id, patient:clin_id"
      )
    )

  }

  it should "return valid document reference" in {

    val files = Map(
      "file1.cram" -> fileEntry(key = "file1.cram"),
      "file1.crai" -> fileEntry(key = "file1.crai"),
      "file2.vcf" -> fileEntry(key = "file2.vcf"),
      "file2.tbi" -> fileEntry(key = "file2.tbi"),
      "file4.vcf" -> fileEntry(key = "file4.vcf"),
      "file4.tbi" -> fileEntry(key = "file4.tbi"),
      "file3.tgz" -> fileEntry(key = "file3.tgz")
    )
    DocumentReferencesValidation.validateFiles(files, defaultAnalysis) shouldBe Valid(
      TDocumentReferences(
        SequencingAlignment(List(CRAM("id", "file1.cram", Some("md5"), 10, "application/octet-stream"), CRAI("id", "file1.crai", Some("md5"), 10, "application/octet-stream"))),
        VariantCalling(List(SNV_VCF("id", "file2.vcf", Some("md5"), 10, "application/octet-stream"), SNV_TBI("id", "file2.tbi", Some("md5"), 10, "application/octet-stream"))),
        CopyNumberVariant(List(CNV_VCF("id", "file4.vcf", Some("md5"), 10, "application/octet-stream"), CNV_TBI("id", "file4.tbi", Some("md5"), 10, "application/octet-stream"))),
        QualityControl(List(QC("id", "file3.tgz", Some("md5"), 10, "application/octet-stream")))
      )
    )

  }


}
