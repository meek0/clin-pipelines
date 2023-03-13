package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.model.TTask
import bio.ferlab.clin.etl.testutils.{FhirTestUtils, WholeStackSuite}
import org.hl7.fhir.r4.model._
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.collection.JavaConverters._
import scala.io.Source

class FileImportFeatureSpec extends FlatSpec with WholeStackSuite with Matchers {

  "run" should "return no errors" in {
    withS3Objects { (inputPrefix, outputPrefix) =>

      transferFromResources(inputPrefix, "good")

      val patientId = FhirTestUtils.loadPatients().getIdPart
      val fhirPatientId = s"Patient/$patientId"
      val serviceRequestId = FhirTestUtils.loadServiceRequest(patientId)
      val fhirServiceRequestId = s"ServiceRequest/$serviceRequestId"
      FhirTestUtils.loadCQGCOrganization()
      val organizationAlias = nextId()

      val organizationId = FhirTestUtils.loadOrganizations(organizationAlias)
      val fhirOrganizationId = s"Organization/$organizationId"
      val ldmSpecimenId = nextId()
      val ldmSampleId = nextId()
      val templateMetadata = Source.fromResource("good/metadata.json").mkString
      val metadata = templateMetadata
        .replace("_PATIENT_ID_", patientId)
        .replace("_SERVICE_REQUEST_ID_", serviceRequestId)
        .replace("_ORGANIZATION_ALIAS_", organizationAlias)
        .replace("_LDM_SPECIMEN_ID_", ldmSpecimenId)
        .replace("_LDM_SAMPLE_ID_", ldmSampleId)
      val putMetadata = PutObjectRequest.builder().bucket(inputBucket).key(s"$inputPrefix/metadata.json").build()
      s3.putObject(putMetadata, RequestBody.fromString(metadata))
      val reportPath = s"$inputPrefix/logs"
      val result = FileImport.run(inputBucket, inputPrefix, outputBucket, outputPrefix, reportPath, dryRun = false, full = false)
      //Validate documents that has been copied

      result.isValid shouldBe true
      val resultFiles = list(outputBucket, outputPrefix)
      resultFiles.size shouldBe 9
      val searchSpecimens = searchFhir("Specimen")
      searchSpecimens.getTotal shouldBe 2
      searchSpecimens.getEntry.asScala.foreach { be =>
        val s = be.getResource.asInstanceOf[Specimen]
        s.getSubject.getReference shouldBe fhirPatientId
        s.getType.getCodingFirstRep.getSystem shouldBe CodingSystems.SPECIMEN_TYPE
        s.getType.getCodingFirstRep.getCode shouldBe "NBL"
      }
      val fullSpecimens = read(searchSpecimens, classOf[Specimen])

      //Validate specimen
      val optSpecimen = fullSpecimens.collectFirst { case s if !s.hasParent => s }
      optSpecimen shouldBe defined
      val specimen = optSpecimen.get
      specimen.getSubject.getReference shouldBe fhirPatientId
      specimen.getRequestFirstRep.getReference shouldBe fhirServiceRequestId
      specimen.getAccessionIdentifier.getSystem shouldBe s"https://cqgc.qc.ca/labs/$organizationAlias/specimen"
      specimen.getAccessionIdentifier.getValue shouldBe ldmSpecimenId
      specimen.getAccessionIdentifier.getAssigner.getReference shouldBe fhirOrganizationId

      //Validate sample
      val optSample = fullSpecimens.collectFirst { case s if s.hasParent && s.getAccessionIdentifier != null && s.getAccessionIdentifier.getSystem == s"https://cqgc.qc.ca/labs/$organizationAlias/sample" => s }
      optSample shouldBe defined
      val sample = optSample.get
      sample.getParentFirstRep.getReference shouldBe id(specimen)
      sample.getSubject.getReference shouldBe fhirPatientId
      sample.getRequestFirstRep.getReference shouldBe fhirServiceRequestId
      sample.getAccessionIdentifier.getValue shouldBe ldmSampleId
      sample.getAccessionIdentifier.getAssigner.getReference shouldBe fhirOrganizationId


      //Validate Service request
      val specimenIds = Seq(id(specimen), id(sample))
      val updatedSr = searchFhir("ServiceRequest")
      updatedSr.getTotal shouldBe 1
      updatedSr.getEntry.asScala.foreach { be =>
        val r = be.getResource.asInstanceOf[ServiceRequest]
        r.getSpecimen.asScala.map(_.getReference) should contain theSameElementsAs specimenIds
      }

      //Validate DocumentReference
      val searchDr = searchFhir("DocumentReference")
      searchDr.getTotal shouldBe 5
      val documentReferences = read(searchDr, classOf[DocumentReference])
      documentReferences.foreach { d =>
        d.getMasterIdentifier.getValue should startWith(outputPrefix)
        d.getContent.asScala.map { content =>
          val attachment = content.getAttachment
          val objectKey = attachment.getUrl.replace(ferloadConf.url, "").replaceFirst("/", "")
          objectKey should startWith(outputPrefix)
          d.getSubject.getReference shouldBe fhirPatientId
          d.getCustodian.getReference shouldBe fhirOrganizationId
          d.getContext.getRelatedFirstRep.getReference shouldBe id(sample)
          d.getContext.getRelatedFirstRep.getDisplay shouldBe s"Submitter Sample ID: $ldmSampleId"
        }
      }
      //Expected title
      documentReferences.flatMap(d => d.getContent.asScala.map(_.getAttachment.getTitle)) should contain only("file1.cram", "file1.crai", "file2.vcf", "file2.tbi", "file4.vcf", "file4.tbi", "file5.vcf", "file5.tbi", "file3.json")

      //Expected code systems
      documentReferences.flatMap(d => d.getType.getCoding.asScala.map(_.getSystem)) should contain only CodingSystems.DR_TYPE
      documentReferences.flatMap(d => d.getType.getCoding.asScala.map(_.getCode)) should contain only("ALIR", "SNV", "GCNV", "GSV", "SSUP")
      documentReferences.map(d => d.getCategoryFirstRep.getCodingFirstRep.getSystem) should contain only CodingSystems.DR_CATEGORY
      documentReferences.map(d => d.getCategoryFirstRep.getCodingFirstRep.getCode) should contain only "GENO"
      documentReferences.flatMap(d => d.getContent.asScala.map(_.getFormat.getSystem)) should contain only CodingSystems.DR_FORMAT

      //Validate tasks
      val searchTasks = searchFhir("Task")
      searchTasks.getTotal shouldBe 1
      val tasks = read(searchTasks, classOf[Task])
      tasks.foreach { t =>
        t.getFor.getReference shouldBe fhirPatientId
        t.getRequester.getReference shouldBe fhirOrganizationId
        t.getOwner.getReference shouldBe "Organization/CQGC"
        t.getFocus.getReference shouldBe fhirServiceRequestId
        t.getOutput.size() shouldBe 4
      }
      tasks.map(_.getCode.getCodingFirstRep.getCode) should contain only TTask.EXOME_GERMLINE_ANALYSIS
      val bundleJson = s"$reportPath/bundle.json"
      assert(S3Utils.exists(inputBucket, bundleJson), s"Bundle json file $bundleJson does not exist")
      val fileCSV = s"$reportPath/files.csv"
      assert(S3Utils.exists(inputBucket, fileCSV), s"File CSV $fileCSV does not exist")
    }


  }
  it should "return errors" in {
    withS3Objects { (inputPrefix, outputPrefix) =>
      transferFromResources(inputPrefix, "bad")
      val reportPath = s"$inputPrefix/logs"
      val result = FileImport.run(inputBucket, inputPrefix, outputBucket, outputPrefix, reportPath, dryRun = false, full = false)

      //Validate documents that has been copied
      result.isValid shouldBe false

    }
  }


}