package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Profiles.{ANALYSIS_SERVICE_REQUEST, SEQUENCING_SERVICE_REQUEST}
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.{CodingSystems, Profiles}
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.model.TTask
import bio.ferlab.clin.etl.testutils.{FhirTestUtils, WholeStackSuite}
import org.hl7.fhir.r4.model._
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.collection.JavaConverters._
import scala.io.Source

class FullFileImportFeatureSpec extends FlatSpec with WholeStackSuite with Matchers {

  "run" should "return no errors" in {
    withS3Objects { (inputPrefix, outputPrefix) =>
      transferFromResources(inputPrefix, "full")

      val ramq = nextId()
      val mrn = nextId()
      val ldmServiceRequestId = nextId()
      val epOrgId = FhirTestUtils.loadOrganizations(id = "CHUS", alias = "CHUS", name = "CHU Sherbroooke")
      val ldmOrgId = FhirTestUtils.loadOrganizations(id = "LDM-CHUS", alias = "LDM-CHUS", name = "LDM CHU Sherbroooke")
      val ldmFhirOrganizationId = s"Organization/$ldmOrgId"
      FhirTestUtils.loadCQGCOrganization()
      val ldmSpecimenId = nextId()
      val ldmSampleId = nextId()
      val templateMetadata = Source.fromResource("full/metadata.json").mkString
      val metadata = templateMetadata
        .replace("_RAMQ_", ramq)
        .replace("_MRN_", mrn)
        .replace("_LDM_ORGANIZATION_ID_", ldmFhirOrganizationId)
        .replace("_EP_ORGANIZATION_ID_", epOrgId)
        .replace("_LDM_SPECIMEN_ID_", ldmSpecimenId)
        .replace("_LDM_SAMPLE_ID_", ldmSampleId)
        .replace("_LDM_SERVICE_REQUEST_ID_", ldmServiceRequestId)
      val putMetadata = PutObjectRequest.builder().bucket(inputBucket).key(s"$inputPrefix/metadata.json").build()
      s3.putObject(putMetadata, RequestBody.fromString(metadata))
      val reportPath = s"$inputPrefix/logs"
      val result = FileImport.run(inputBucket, inputPrefix, outputBucket, outputPrefix, reportPath, dryRun = false, full = true)

      result.isValid shouldBe true
      val resultFiles = list(outputBucket, outputPrefix)
      resultFiles.size shouldBe 7

      val searchPatients = searchFhir("Patient")
      searchPatients.getTotal shouldBe 1
      val patients = read(searchPatients, classOf[Patient])
      val patient = patients.head
      patient.getIdentifierFirstRep.getValue shouldBe mrn
      val patientId = id(patients.head)

      val searchPersons = searchFhir("Person")
      searchPersons.getTotal shouldBe 1
      val persons = read(searchPersons, classOf[Person])
      val person = persons.head
      person.getIdentifierFirstRep.getValue shouldBe ramq
      person.getLinkFirstRep.getTarget.getReference shouldBe patientId

      val searchSpecimens = searchFhir("Specimen")
      searchSpecimens.getTotal shouldBe 2
      searchSpecimens.getEntry.asScala.foreach { be =>
        val s = be.getResource.asInstanceOf[Specimen]
        s.getSubject.getReference shouldBe patientId
        s.getType.getCodingFirstRep.getSystem shouldBe CodingSystems.SPECIMEN_TYPE
        s.getType.getCodingFirstRep.getCode shouldBe "NBL"
      }
      val fullSpecimens = read(searchSpecimens, classOf[Specimen])

      //Validate specimen
      val optSpecimen = fullSpecimens.collectFirst { case s if !s.hasParent => s }
      optSpecimen shouldBe defined
      val specimen = optSpecimen.get
      specimen.getSubject.getReference shouldBe patientId
      specimen.getAccessionIdentifier.getSystem shouldBe s"https://cqgc.qc.ca/labs/$ldmOrgId/specimen"
      specimen.getAccessionIdentifier.getValue shouldBe ldmSpecimenId
      specimen.getAccessionIdentifier.getAssigner.getReference shouldBe ldmFhirOrganizationId

      //Validate sample
      val optSample = fullSpecimens.collectFirst { case s if s.hasParent && s.getAccessionIdentifier != null && s.getAccessionIdentifier.getSystem == s"https://cqgc.qc.ca/labs/$ldmOrgId/sample" => s }
      optSample shouldBe defined
      val sample = optSample.get
      sample.getParentFirstRep.getReference shouldBe id(specimen)
      sample.getSubject.getReference shouldBe patientId

      sample.getAccessionIdentifier.getValue shouldBe ldmSampleId
      sample.getAccessionIdentifier.getAssigner.getReference shouldBe ldmFhirOrganizationId


      //Validate Service request
      val specimenIds = Seq(id(specimen), id(sample))
      val searchServiceRequests = searchFhir("ServiceRequest")
      val serviceRequests = read(searchServiceRequests, classOf[ServiceRequest])
      val analysisServiceRequests: Seq[ServiceRequest] = serviceRequests.filter {
        r => r.getMeta.hasProfile(ANALYSIS_SERVICE_REQUEST)
      }
      analysisServiceRequests.size shouldBe 1

      val analysisServiceRequest = analysisServiceRequests.head
      analysisServiceRequest.getSpecimen.asScala shouldBe empty
      analysisServiceRequest.getSubject.getReference shouldBe patientId
      analysisServiceRequest.getCode.getCodingFirstRep.getSystem shouldBe CodingSystems.ANALYSIS_REQUEST_CODE
      analysisServiceRequest.getCode.getCodingFirstRep.getCode shouldBe "MMG"
      analysisServiceRequest.getPerformerFirstRep.getReference shouldBe ldmFhirOrganizationId

      val analysisServiceRequestId = id(analysisServiceRequest)

      val sequencingServiceRequests: Seq[ServiceRequest] = serviceRequests.filter {
        r => r.getMeta.hasProfile(SEQUENCING_SERVICE_REQUEST)
      }
      sequencingServiceRequests.size shouldBe 1
      sequencingServiceRequests.foreach { r =>
        r.getSpecimen.asScala.map(_.getReference) should contain theSameElementsAs specimenIds
        r.getBasedOnFirstRep.getReference shouldBe analysisServiceRequestId
        r.getSubject.getReference shouldBe patientId
        r.getCode.getCodingFirstRep.getSystem shouldBe CodingSystems.ANALYSIS_REQUEST_CODE
        r.getCode.getCodingFirstRep.getCode shouldBe "MMG"
        r.getPerformerFirstRep.getReference shouldBe ldmFhirOrganizationId
      }
      val sequencingServiceRequestId = id(sequencingServiceRequests.head)
      sample.getRequestFirstRep.getReference shouldBe sequencingServiceRequestId
      specimen.getRequestFirstRep.getReference shouldBe sequencingServiceRequestId


      //Validate DocumentReference
      val searchDr = searchFhir("DocumentReference")
      searchDr.getTotal shouldBe 4
      val documentReferences = read(searchDr, classOf[DocumentReference])
      documentReferences.foreach { d =>
        d.getMasterIdentifier.getValue should startWith(outputPrefix)
        d.getContent.asScala.map { content =>
          val attachment = content.getAttachment
          val objectKey = attachment.getUrl.replace(ferloadConf.url, "").replaceFirst("/", "")
          objectKey should startWith(outputPrefix)
          d.getSubject.getReference shouldBe patientId
          d.getCustodian.getReference shouldBe ldmFhirOrganizationId
          d.getContext.getRelatedFirstRep.getReference shouldBe id(sample)
          d.getContext.getRelatedFirstRep.getDisplay shouldBe s"Submitter Sample ID: $ldmSampleId"
        }
      }
      //Expected title
      documentReferences.flatMap(d => d.getContent.asScala.map(_.getAttachment.getTitle)) should contain only("file1.cram", "file1.crai", "file2.vcf", "file2.tbi", "file4.vcf", "file4.tbi", "file3.json")

      //Expected code systems
      documentReferences.flatMap(d => d.getType.getCoding.asScala.map(_.getSystem)) should contain only CodingSystems.DR_TYPE
      documentReferences.flatMap(d => d.getType.getCoding.asScala.map(_.getCode)) should contain only("ALIR", "SNV", "GCNV", "SSUP")
      documentReferences.map(d => d.getCategoryFirstRep.getCodingFirstRep.getSystem) should contain only CodingSystems.DR_CATEGORY
      documentReferences.map(d => d.getCategoryFirstRep.getCodingFirstRep.getCode) should contain only "GENO"
      documentReferences.flatMap(d => d.getContent.asScala.map(_.getFormat.getSystem)) should contain only CodingSystems.DR_FORMAT

      //Validate tasks
      val searchTasks = searchFhir("Task")
      searchTasks.getTotal shouldBe 1
      val tasks = read(searchTasks, classOf[Task])
      tasks.foreach { t =>
        t.getFor.getReference shouldBe patientId
        t.getRequester.getReference shouldBe ldmFhirOrganizationId
        t.getOwner.getReference shouldBe "Organization/CQGC"
        t.getFocus.getReference shouldBe sequencingServiceRequestId
        t.getOutput.size() shouldBe 4
      }
      tasks.map(_.getCode.getCodingFirstRep.getCode) should contain only TTask.EXOME_GERMLINE_ANALYSIS

      //Valid ClinicalImpression
      val searchClinicalImpressions = searchFhir("ClinicalImpression")
      searchClinicalImpressions.getTotal shouldBe 1
      val clinicalImpression = read(searchClinicalImpressions, classOf[ClinicalImpression]).head
      clinicalImpression.getSubject.getReference shouldBe patientId

      //Valid Observation
      val searchObservations = searchFhir("Observation")
      searchObservations.getTotal shouldBe 1
      val diseaseStatus = read(searchObservations, classOf[Observation]).head
      diseaseStatus.getSubject.getReference shouldBe patientId

      clinicalImpression.getInvestigation.size() shouldBe 1
      clinicalImpression.getInvestigationFirstRep.getCode.getText shouldBe "Examination / signs"
      clinicalImpression.getInvestigationFirstRep.getItem.size() shouldBe 1
      clinicalImpression.getInvestigationFirstRep.getItemFirstRep.getReference shouldBe id(diseaseStatus)

      analysisServiceRequest.getSupportingInfo.size() shouldBe 1
      analysisServiceRequest.getSupportingInfoFirstRep.getReference shouldBe id(clinicalImpression)

      //Validate documents that has been copied
      val bundleJson = s"$reportPath/bundle.json"
      assert(S3Utils.exists(inputBucket, bundleJson), s"Bundle json file $bundleJson does not exist")
      val fileCSV = s"$reportPath/files.csv"
      assert(S3Utils.exists(inputBucket, fileCSV), s"File CSV $fileCSV does not exist")
    }


  }


}