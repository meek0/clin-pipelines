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
      FileEntry("file1.cram", "123", "md5", 10, "1"),
      FileEntry("file1.crai", "345", "md5", 10, "1.crai"),
      FileEntry("file2.vcf", "678", "md5", 10, "2"),
      FileEntry("file2.tbi", "901", "md5", 10, "2.tbi"),
      FileEntry("file3.tgz", "234", "md5", 10, "3")
    )
    val result = BuildBundle.validate(meta, files)

    val saveResult = result.map(b =>
      b.save()

    )
    println(saveResult)

  }

}
