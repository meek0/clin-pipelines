package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.task.ldmnotifier.{TasksTransformer => Transformer}
import bio.ferlab.clin.etl.task.ldmnotifier.model.{Attachment, Content, Document, ManifestRow, Owner, Sample, Task}
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.{FlatSpec, GivenWhenThen}

class TasksTransformerSpec extends FlatSpec with GivenWhenThen {
  def makeFakeTask(): Seq[Task] = {
    val Task1Ldm1 = Task(
      id = "task1",
      serviceRequestReference = "ServiceRequest/1",
      owner = Owner(id = "LDM1", alias = "LDM1", email = "LDM1@mail.com"),
      documents = Seq(
        Document(
          contentList = Seq(
            Content(
              attachment = Attachment(
                url = "https://ferload.env.clin.ferlab.bio/ldm1file1",
                hash64 = Some("OWY3Y2Y5YzFjN2E2MWU2NDVkM2MzYzAyYmFhYzI0MGM="),
                title = "16900.cram.cram"
              ),
              fileFormat = "CRAM"
            ),
            Content(
              attachment = Attachment(
                url = "https://ferload.env.clin.ferlab.bio/ldm1file1.crai",
                hash64 = None,
                title = "16900.cram.crai"
              ),
              fileFormat = "CRAI"
            )
          ),
          sample = Sample(sampleId = "sampleId"),
          patientReference = "Patient/1",
          fileType = "AR"
        )
      )
    )
    val Task2Ldm1 = Task(
      id = "task2",
      owner = Owner(id = "LDM1", alias = "LDM1", email = "LDM1@mail.com"),
      serviceRequestReference = "ServiceRequest/2",
      documents = Seq(
        Document(
          contentList = Seq(
            Content(
              attachment = Attachment(
                url = "https://ferload.env.clin.ferlab.bio/ldm1file2",
                hash64 = Some("YWMzN2RhMWViOTgwMzQ3NzQ2NWU5ZjgwZDM0NzE2ZmQ"),
                title = "16870.hard-filtered.gvcf.gz"
              ),
              fileFormat = "VCF"
            ),
            Content(
              attachment = Attachment(
                url = "https://ferload.env.clin.ferlab.bio/ldm1file2.tbi",
                hash64 = None,
                title = "16870.hard-filtered.gvcf.gz.tbi"
              ),
              fileFormat = "TBI"
            )
          ),
          sample = Sample(sampleId = "sampleId"),
          patientReference = "Patient/2",
          fileType = "SNV"
        )
      )
    )
    val Task3Ldm2 = Task(
      id = "task3",
      owner = Owner(id = "LDM2", alias = "LDM2", email = "LDM2@mail.com"),
      serviceRequestReference = "ServiceRequest/3",
      documents = Seq(
        Document(
          contentList = Seq(
            Content(
              attachment = Attachment(
                url = "https://ferload.env.clin.ferlab.bio/ldm2file1.tbi",
                hash64 = None,
                title = "16870.QC.tgz"
              ),
              fileFormat = "TGZ"
            )
          ),
          sample = Sample(sampleId = "sampleId"),
          patientReference = "Patient/3",
          fileType = "QC"
        )
      )
    )
    Seq(Task1Ldm1, Task2Ldm1, Task3Ldm2)
  }

  it should "group all needed information by alias and their email(s)" in {
    Given("Parsed tasks")
    val tasks = makeFakeTask()

    Then("grouping them by aliases should give the correct size")
    val group = Transformer.groupManifestRowsByLdm("https://clin.bio", tasks)
    group.size shouldBe 2

    And("have correct keys (alias, email)")
    group.contains(("LDM1", "LDM1@mail.com")) shouldBe true
    group.contains(("LDM2", "LDM2@mail.com")) shouldBe true

    And("each alias should have the corresponding data")
    group(("LDM1", "LDM1@mail.com")) shouldBe Seq(
      ManifestRow(
        url = "/ldm1file1",
        fileName = "16900.cram.cram",
        fileType = "AR",
        fileFormat = "CRAM",
        hash = Some("9f7cf9c1c7a61e645d3c3c02baac240c"),
        ldmSampleId = "sampleId",
        patientId = "1",
        serviceRequestId = "1",
        cqgcLink = "https://clin.bio/patient/1"
      ),
      ManifestRow(
        url = "/ldm1file1.crai",
        fileName = "16900.cram.crai",
        fileType = "AR",
        fileFormat = "CRAI",
        hash = None,
        ldmSampleId = "sampleId",
        patientId = "1",
        serviceRequestId = "1",
        cqgcLink = "https://clin.bio/patient/1"
      ),
      ManifestRow(
        url = "/ldm1file2",
        fileName = "16870.hard-filtered.gvcf.gz",
        fileType = "SNV",
        fileFormat = "VCF",
        hash = Some("ac37da1eb9803477465e9f80d34716fd"),
        ldmSampleId = "sampleId",
        patientId = "2",
        serviceRequestId = "2",
        cqgcLink = "https://clin.bio/patient/2"
      ),
      ManifestRow(
        url = "/ldm1file2.tbi",
        fileName = "16870.hard-filtered.gvcf.gz.tbi",
        fileType = "SNV",
        fileFormat = "TBI",
        hash = None,
        ldmSampleId = "sampleId",
        patientId = "2",
        serviceRequestId = "2",
        cqgcLink = "https://clin.bio/patient/2"
      )
    )
    group(("LDM2", "LDM2@mail.com")) shouldBe Seq(
      ManifestRow(
        url = "/ldm2file1.tbi",
        fileName = "16870.QC.tgz",
        fileType = "QC",
        fileFormat = "TGZ",
        hash = None,
        ldmSampleId = "sampleId",
        patientId = "3",
        serviceRequestId = "3",
        cqgcLink = "https://clin.bio/patient/3"
      )
    )
  }
}
