package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils.{defaultAnalysis, defaultMetadata, defaultPatient}
import bio.ferlab.clin.etl.fhir.testutils.{FhirServerSuite, FhirTestUtils}
import bio.ferlab.clin.etl.model.FileEntry
import bio.ferlab.clin.etl.task.BuildBundle
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class BuildBundleSpec extends FlatSpec with Matchers with GivenWhenThen with FhirServerSuite {

  "it" should "Build" in {
    val ptId = FhirTestUtils.loadPatients().getIdPart
    val orgId = FhirTestUtils.loadOrganizations()
    val serviceRequestId = FhirTestUtils.loadServiceRequest(ptId)
    val meta = defaultMetadata.copy(analyses = Seq(
      defaultAnalysis.copy(patient = defaultPatient(ptId), serviceRequestId = serviceRequestId)
    ))
    val files = Seq(
      FileEntry("file1.cram", "123", Some("md5"), 10, "1", "application/octet-stream", ""),
      FileEntry("file1.crai", "345", Some("md5"), 10, "1.crai", "application/octet-stream", ""),
      FileEntry("file2.vcf", "678", Some("md5"), 10, "2", "application/octet-stream", ""),
      FileEntry("file2.tbi", "901", Some("md5"), 10, "2.tbi", "application/octet-stream", ""),
      FileEntry("file3.tgz", "234", Some("md5"), 10, "3", "application/octet-stream", "")
    )
    val result = BuildBundle.validate(meta, files)

    val saveResult = result.map(b =>
      b.save()

    )
    println(saveResult)

  }

}
