package bio.ferlab.clin.etl.task.fileimport.validation

import bio.ferlab.clin.etl.task.fileimport.model._
import bio.ferlab.clin.etl.testutils.FhirServerSuite
import bio.ferlab.clin.etl.testutils.MetadataTestUtils.defaultAnalysis
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
      "file4.tbi" -> fileEntry(key = "file4.tbi"),
      "file5.vcf" -> fileEntry(key = "file5.vcf"),
      "file5.tbi" -> fileEntry(key = "file5.tbi"),
      "file6.html" -> fileEntry(key = "file6.html"),
      "file6.json" -> fileEntry(key = "file6.json"),
      "file6.variants.tsv" -> fileEntry(key = "file6.variants.tsv"),
      "file7.seg.bw" -> fileEntry(key = "file7.seg.bw"),
      "file7.baf.bw" -> fileEntry(key = "file7.baf.bw"),
      "file7.roh.bed" -> fileEntry(key = "file7.roh.bed"),
      "file7.exome.bed" -> fileEntry(key = "file7.exome.bed"),
      "file8.png" -> fileEntry(key = "file8.png"),
      "file9.csv" -> fileEntry(key = "file9.csv"),
      "file10.json" -> fileEntry(key = "file10.json"),
    )
    DocumentReferencesValidation.validateFiles(files, defaultAnalysis) shouldBe Invalid(
      NonEmptyList.of(
        s"File file2.tbi does not exist : type=tbi, specimen=submitted_specimen_id, sample=submitted_sample_id",
        "File file3.tgz does not exist : type=supplement, specimen=submitted_specimen_id, sample=submitted_sample_id"
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
      "file5.vcf" -> fileEntry(key = "file5.vcf"),
      "file5.tbi" -> fileEntry(key = "file5.tbi"),
      "file3.tgz" -> fileEntry(key = "file3.tgz"),
      "file6.html" -> fileEntry(key = "file6.html"),
      "file6.json" -> fileEntry(key = "file6.json"),
      "file6.variants.tsv" -> fileEntry(key = "file6.variants.tsv"),
      "file7.seg.bw" -> fileEntry(key = "file7.seg.bw"),
      "file7.baf.bw" -> fileEntry(key = "file7.baf.bw"),
      "file7.roh.bed" -> fileEntry(key = "file7.roh.bed"),
      "file7.exome.bed" -> fileEntry(key = "file7.exome.bed"),
      "file8.png" -> fileEntry(key = "file8.png"),
      "file9.csv" -> fileEntry(key = "file9.csv"),
      "file10.json" -> fileEntry(key = "file10.json"),
    )
    DocumentReferencesValidation.validateFiles(files, defaultAnalysis) shouldBe Valid(
      TDocumentReferences(
        SequencingAlignment(List(CRAM("id", "file1.cram", Some("md5"), 10, "application/octet-stream"), CRAI("id", "file1.crai", Some("md5"), 10, "application/octet-stream"))),
        VariantCalling(List(SNV_VCF("id", "file2.vcf", Some("md5"), 10, "application/octet-stream"), SNV_TBI("id", "file2.tbi", Some("md5"), 10, "application/octet-stream"))),
        CopyNumberVariant(List(CNV_VCF("id", "file4.vcf", Some("md5"), 10, "application/octet-stream"), CNV_TBI("id", "file4.tbi", Some("md5"), 10, "application/octet-stream"))),
        Some(StructuralVariant(List(SV_VCF("id", "file5.vcf", Some("md5"), 10, "application/octet-stream"), SV_TBI("id", "file5.tbi", Some("md5"), 10, "application/octet-stream")))),
        SupplementDocument(List(Supplement("id", "file3.tgz", Some("md5"), 10, "application/octet-stream"))),
        Some(Exomiser(List(EXOMISER_HTML("id", "file6.html", Some("md5"), 10, "application/octet-stream"), EXOMISER_JSON("id", "file6.json", Some("md5"), 10, "application/octet-stream"), EXOMISER_VARIANTS_TSV("id", "file6.variants.tsv", Some("md5"), 10, "application/octet-stream")))),
        Some(IgvTrack(List(SEG_BW("id", "file7.seg.bw", Some("md5"), 10, "application/octet-stream"), HARD_FILTERED_BAF_BW("id", "file7.baf.bw", Some("md5"), 10, "application/octet-stream"), ROH_BED("id", "file7.roh.bed", Some("md5"), 10, "application/octet-stream"), HYPER_EXOME_HG38_BED("id", "file7.exome.bed", Some("md5"), 10, "application/octet-stream")))),
        Some(CnvVisualization(List(CNV_CALLS_PNG("id", "file8.png", Some("md5"),10, "application/octet-stream")))),
        Some(CoverageByGene(List(COVERAGE_BY_GENE_CSV("id", "file9.csv", Some("md5"),10, "application/octet-stream")))),
        Some(QcMetrics(List(QC_METRICS("id", "file10.json", Some("md5"),10, "application/octet-stream")))),
      )
    )

  }


}