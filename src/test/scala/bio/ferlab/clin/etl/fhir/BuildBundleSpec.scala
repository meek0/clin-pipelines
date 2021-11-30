package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils.{defaultAnalysis, defaultMetadata, defaultPatient}
import bio.ferlab.clin.etl.fhir.testutils.{FhirServerSuite, FhirTestUtils}
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, TBundle}
import bio.ferlab.clin.etl.task.fileimport.BuildBundle
import cats.data.Validated.Valid
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class BuildBundleSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {

  "it" should "Build" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    FhirTestUtils.loadOrganizations()
    FhirTestUtils.loadCQGCOrganization()
    val serviceRequestId = FhirTestUtils.loadServiceRequest(ptId)
    val meta = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId), clinServiceRequestId = serviceRequestId)
    ))
    val files = Seq(
      FileEntry("bucket","file1.cram", Some("md5"), 10, "1", "application/octet-stream", ""),
      FileEntry("bucket","file1.crai", Some("md5"), 10, "1.crai", "application/octet-stream", ""),
      FileEntry("bucket","file2.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket","file2.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("bucket","file4.vcf", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("bucket","file4.tbi", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("bucket","file3.tgz", Some("md5"), 10, "3", "application/octet-stream", "")
    )
    val result: ValidationResult[TBundle] = BuildBundle.validate(meta, files)

    result.isValid shouldBe true


  }

}
