package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Profiles.{ANALYSIS_SERVICE_REQUEST, SEQUENCING_SERVICE_REQUEST}
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.model.TTask
import bio.ferlab.clin.etl.testutils.{FhirTestUtils, WholeStackSuite}
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender
import org.hl7.fhir.r4.model._
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.collection.JavaConverters._
import scala.io.Source

class FullFamilyFileImportFeatureSpec extends FlatSpec with WholeStackSuite with Matchers {

  "run" should "return no errors" in {
    withS3Objects { (inputPrefix, outputPrefix) =>
      transferFromResources(inputPrefix, "full_family")

      val ramqProb = nextId()
      val mrnProb = nextId()
      val ramqMth = nextId()
      val mrnMth = nextId()
      val ldmServiceRequestId = nextId()
      val epOrgId = FhirTestUtils.loadOrganizations(id = "CHUS", alias = "CHUS", name = "CHU Sherbroooke")
      val ldmOrgId = FhirTestUtils.loadOrganizations(id = "LDM-CHUS", alias = "LDM-CHUS", name = "LDM CHU Sherbroooke")
      FhirTestUtils.loadCQGCOrganization()
      val ldmFhirOrganizationId = s"Organization/$ldmOrgId"
      val ldmProbSpecimenId = nextId()
      val ldmProbSampleId = nextId()
      val ldmMthSpecimenId = nextId()
      val ldmMthSampleId = nextId()
      val templateMetadata = Source.fromResource("full_family/metadata.json").mkString
      val metadata = templateMetadata
        .replace("_RAMQ_PROB_", ramqProb)
        .replace("_MRN_PROB_", mrnProb)
        .replace("_RAMQ_MTH_", ramqMth)
        .replace("_MRN_MTH_", mrnMth)
        .replace("_LDM_ORGANIZATION_ID_", ldmFhirOrganizationId)
        .replace("_EP_ORGANIZATION_ID_", epOrgId)
        .replace("_LDM_PROB_SPECIMEN_ID_", ldmProbSpecimenId)
        .replace("_LDM_PROB_SAMPLE_ID_", ldmProbSampleId)
        .replace("_LDM_MTH_SPECIMEN_ID_", ldmMthSpecimenId)
        .replace("_LDM_MTH_SAMPLE_ID_", ldmMthSampleId)
        .replace("_LDM_SERVICE_REQUEST_ID_", ldmServiceRequestId)
      val putMetadata = PutObjectRequest.builder().bucket(inputBucket).key(s"$inputPrefix/metadata.json").build()
      s3.putObject(putMetadata, RequestBody.fromString(metadata))
      val reportPath = s"$inputPrefix/logs"
      val result = FileImport.run(inputBucket, inputPrefix, outputBucket, outputPrefix, reportPath, dryRun = false, full = true)

      result.isValid shouldBe true
      val resultFiles = list(outputBucket, outputPrefix)
      resultFiles.size shouldBe 19

      val searchPatients = searchFhir("Patient")
      searchPatients.getTotal shouldBe 2
      val patients = read(searchPatients, classOf[Patient])

      val optProbandPatient: Option[Patient] = patients.find(_.getIdentifierFirstRep.getValue == mrnProb)
      optProbandPatient.isDefined shouldBe true
      val probandPatient = optProbandPatient.get
      val probandPatientId = id(probandPatient)

      val optMotherPatient: Option[Patient] = patients.find(_.getIdentifierFirstRep.getValue == mrnMth)
      optMotherPatient.isDefined shouldBe true
      val motherPatient = optMotherPatient.get
      val motherPatientId = id(motherPatient)

      val searchPersons = searchFhir("Person")
      searchPersons.getTotal shouldBe 2
      val persons = read(searchPersons, classOf[Person])

      val optProbandPerson: Option[Person] = persons.find(_.getIdentifierFirstRep.getValue == ramqProb)
      optProbandPerson.isDefined shouldBe true
      val probandPerson = optProbandPerson.get
      probandPerson.getNameFirstRep.getFamily shouldBe "Doe"
      probandPerson.getNameFirstRep.getGivenAsSingleString shouldBe "John"
      probandPerson.getGender shouldBe AdministrativeGender.MALE
      probandPerson.getLinkFirstRep.getTarget.getReference shouldBe probandPatientId

      val optMthPerson: Option[Person] = persons.find(_.getIdentifierFirstRep.getValue == ramqMth)
      optMthPerson.isDefined shouldBe true
      val motherPerson = optMthPerson.get
      motherPerson.getNameFirstRep.getFamily shouldBe "Doe"
      motherPerson.getNameFirstRep.getGivenAsSingleString shouldBe "Jane"
      motherPerson.getGender shouldBe AdministrativeGender.FEMALE
      motherPerson.getLinkFirstRep.getTarget.getReference shouldBe motherPatientId

      val searchSpecimens = searchFhir("Specimen")
      searchSpecimens.getTotal shouldBe 4
      val fullSpecimens = read(searchSpecimens, classOf[Specimen])
      fullSpecimens.foreach { s =>
        s.getType.getCodingFirstRep.getSystem shouldBe CodingSystems.SPECIMEN_TYPE
        s.getType.getCodingFirstRep.getCode shouldBe "NBL"
      }

      //Proband Specimen
      val probandAllSpecimens = fullSpecimens.filter(_.getSubject.getReference == probandPatientId)
      probandAllSpecimens.size shouldBe 2

      //Validate specimen
      val optProbandSpecimen = probandAllSpecimens.collectFirst { case s if !s.hasParent => s }
      optProbandSpecimen shouldBe defined
      val probandSpecimen = optProbandSpecimen.get
      probandSpecimen.getSubject.getReference shouldBe probandPatientId
      probandSpecimen.getAccessionIdentifier.getSystem shouldBe s"https://cqgc.qc.ca/labs/$ldmOrgId/specimen"
      probandSpecimen.getAccessionIdentifier.getValue shouldBe ldmProbSpecimenId
      probandSpecimen.getAccessionIdentifier.getAssigner.getReference shouldBe ldmFhirOrganizationId

      //Validate sample
      val optProbandSample = probandAllSpecimens.collectFirst { case s if s.hasParent && s.getAccessionIdentifier != null && s.getAccessionIdentifier.getSystem == s"https://cqgc.qc.ca/labs/$ldmOrgId/sample" => s }
      optProbandSample shouldBe defined
      val probandSample = optProbandSample.get
      probandSample.getParentFirstRep.getReference shouldBe id(probandSpecimen)
      probandSample.getSubject.getReference shouldBe probandPatientId
      probandSample.getAccessionIdentifier.getValue shouldBe ldmProbSampleId
      probandSample.getAccessionIdentifier.getAssigner.getReference shouldBe ldmFhirOrganizationId

      //MOther Specimen
      val motherAllSpecimens = fullSpecimens.filter(_.getSubject.getReference == motherPatientId)
      motherAllSpecimens.size shouldBe 2

      //Validate specimen
      val optMotherSpecimen = motherAllSpecimens.collectFirst { case s if !s.hasParent => s }
      optMotherSpecimen shouldBe defined
      val motherSpecimen = optMotherSpecimen.get
      motherSpecimen.getSubject.getReference shouldBe motherPatientId
      motherSpecimen.getAccessionIdentifier.getSystem shouldBe s"https://cqgc.qc.ca/labs/$ldmOrgId/specimen"
      motherSpecimen.getAccessionIdentifier.getValue shouldBe ldmMthSpecimenId
      motherSpecimen.getAccessionIdentifier.getAssigner.getReference shouldBe ldmFhirOrganizationId

      //Validate sample
      val optMotherSample = motherAllSpecimens.collectFirst { case s if s.hasParent && s.getAccessionIdentifier != null && s.getAccessionIdentifier.getSystem == s"https://cqgc.qc.ca/labs/$ldmOrgId/sample" => s }
      optMotherSample shouldBe defined
      val motherSample = optMotherSample.get
      motherSample.getParentFirstRep.getReference shouldBe id(motherSpecimen)
      motherSample.getSubject.getReference shouldBe motherPatientId
      motherSample.getAccessionIdentifier.getValue shouldBe ldmMthSampleId
      motherSample.getAccessionIdentifier.getAssigner.getReference shouldBe ldmFhirOrganizationId

      //Validate Service request
      val searchServiceRequests = searchFhir("ServiceRequest")
      val serviceRequests = read(searchServiceRequests, classOf[ServiceRequest])
      serviceRequests.size shouldBe 3

      val analysisServiceRequests: Seq[ServiceRequest] = serviceRequests.filter {
        r => r.getMeta.hasProfile(ANALYSIS_SERVICE_REQUEST)
      }
      analysisServiceRequests.size shouldBe 1

      val analysisServiceRequest = analysisServiceRequests.head
      analysisServiceRequest.getSpecimen.asScala shouldBe empty
      analysisServiceRequest.getSubject.getReference shouldBe probandPatientId
      analysisServiceRequest.getCode.getCodingFirstRep.getSystem shouldBe CodingSystems.ANALYSIS_REQUEST_CODE
      analysisServiceRequest.getCode.getCodingFirstRep.getCode shouldBe "MMG"
      val familyExtension = analysisServiceRequest.getExtensionByUrl("http://fhir.cqgc.ferlab.bio/StructureDefinition/family-member")
      familyExtension.getExtensionByUrl("parent").getValue.asInstanceOf[Reference].getReference shouldBe motherPatientId
      familyExtension.getExtensionByUrl("parent-relationship").getValue.asInstanceOf[CodeableConcept].getCodingFirstRep.getCode  shouldBe "MTH"
      val analysisServiceRequestId = id(analysisServiceRequest)

      val sequencingServiceRequests: Seq[ServiceRequest] = serviceRequests.filter {
        r => r.getMeta.hasProfile(SEQUENCING_SERVICE_REQUEST)
      }
      sequencingServiceRequests.size shouldBe 2
      sequencingServiceRequests.foreach { r =>
        r.getBasedOnFirstRep.getReference shouldBe analysisServiceRequestId
        r.getCode.getCodingFirstRep.getSystem shouldBe CodingSystems.ANALYSIS_REQUEST_CODE
        r.getCode.getCodingFirstRep.getCode shouldBe "MMG"
      }
      val optProbandSequencingServiceRequest = sequencingServiceRequests.find(_.getSubject.getReference == probandPatientId)
      optProbandSequencingServiceRequest.isDefined shouldBe true
      val probandSequencingServiceRequest = optProbandSequencingServiceRequest.get
      probandSequencingServiceRequest.getSpecimen.asScala.map(_.getReference) should contain theSameElementsAs  Seq(id(probandSpecimen), id(probandSample))
      val probandSequencingServiceRequestId = id(probandSequencingServiceRequest)
      probandSample.getRequestFirstRep.getReference shouldBe probandSequencingServiceRequestId
      probandSpecimen.getRequestFirstRep.getReference shouldBe probandSequencingServiceRequestId

      val optMthSequencingServiceRequest = sequencingServiceRequests.find(_.getSubject.getReference == motherPatientId)
      optMthSequencingServiceRequest.isDefined shouldBe true
      val mthSequencingServiceRequest = optMthSequencingServiceRequest.get
      mthSequencingServiceRequest.getSpecimen.asScala.map(_.getReference) should contain theSameElementsAs  Seq(id(motherSpecimen), id(motherSample))
      val mthSequencingServiceRequestId = id(mthSequencingServiceRequest)
      motherSample.getRequestFirstRep.getReference shouldBe mthSequencingServiceRequestId
      motherSpecimen.getRequestFirstRep.getReference shouldBe mthSequencingServiceRequestId


      //Validate DocumentReference
      val searchDr = searchFhir("DocumentReference")
      searchDr.getTotal shouldBe 10
      val documentReferences = read(searchDr, classOf[DocumentReference])
      documentReferences.foreach { d =>
        d.getMasterIdentifier.getValue should startWith(outputPrefix)
        d.getContent.asScala.map { content =>
          val attachment = content.getAttachment
          val objectKey = attachment.getUrl.replace(ferloadConf.url, "").replaceFirst("/", "")
          objectKey should startWith(outputPrefix)
          d.getCustodian.getReference shouldBe ldmFhirOrganizationId
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

      val probandDocumentReferences = documentReferences.filter(_.getSubject.getReference == probandPatientId)
      probandDocumentReferences.size shouldBe 5
      probandDocumentReferences.foreach{ d=>
        d.getContext.getRelatedFirstRep.getReference shouldBe id(probandSample)
        d.getContext.getRelatedFirstRep.getDisplay shouldBe s"Submitter Sample ID: $ldmProbSampleId"
      }
      val motherDocumentReferences = documentReferences.filter(_.getSubject.getReference == motherPatientId)
      motherDocumentReferences.size shouldBe 5
      motherDocumentReferences.foreach{ d=>
        d.getContext.getRelatedFirstRep.getReference shouldBe id(motherSample)
        d.getContext.getRelatedFirstRep.getDisplay shouldBe s"Submitter Sample ID: $ldmMthSampleId"
      }
      //Validate tasks
      val searchTasks = searchFhir("Task")
      searchTasks.getTotal shouldBe 2
      val tasks = read(searchTasks, classOf[Task])
      tasks.foreach { t =>
        t.getRequester.getReference shouldBe ldmFhirOrganizationId
        t.getOwner.getReference shouldBe "Organization/CQGC"
        t.getOutput.size() shouldBe 4
      }
      tasks.map(_.getCode.getCodingFirstRep.getCode) should contain only TTask.EXOME_GERMLINE_ANALYSIS

      val probandTasks = tasks.filter(_.getFor.getReference == probandPatientId)
      probandTasks.size shouldBe 1
      probandTasks.head.getFocus.getReference shouldBe probandSequencingServiceRequestId

      val motherTasks = tasks.filter(_.getFor.getReference == motherPatientId)
      motherTasks.size shouldBe 1
      motherTasks.head.getFocus.getReference shouldBe mthSequencingServiceRequestId

      //Valid ClinicalImpression
      val searchClinicalImpressions = searchFhir("ClinicalImpression")
      searchClinicalImpressions.getTotal shouldBe 2
      val clinicalImpressions = read(searchClinicalImpressions, classOf[ClinicalImpression])
      val optProbandClinicalImpression = clinicalImpressions.find(_.getSubject.getReference == probandPatientId)
      optProbandClinicalImpression shouldBe defined
      val probandClinicalImpression = optProbandClinicalImpression.get
      val optMthClinicalImpression = clinicalImpressions.find(_.getSubject.getReference == motherPatientId)
      optMthClinicalImpression shouldBe defined
      val mthClinicalImpression = optMthClinicalImpression.get

      //Valid Observation
      val searchObservations = searchFhir("Observation")
      searchObservations.getTotal shouldBe 2
      val diseaseStatuses = read(searchObservations, classOf[Observation])

      val optProbandDiseaseStatus = diseaseStatuses.find(_.getSubject.getReference == probandPatientId)
      optProbandDiseaseStatus shouldBe defined
      val probandDiseaseStatus = optProbandDiseaseStatus.get
      probandDiseaseStatus.getInterpretation.size() shouldBe 1
      probandDiseaseStatus.getInterpretationFirstRep.getCodingFirstRep.getCode shouldBe "POS"

      val optMthDiseaseStatus = diseaseStatuses.find(_.getSubject.getReference == motherPatientId)
      optMthDiseaseStatus shouldBe defined
      val mthDiseaseStatus = optMthDiseaseStatus.get
      mthDiseaseStatus.getInterpretation.size() shouldBe 1
      mthDiseaseStatus.getInterpretationFirstRep.getCodingFirstRep.getCode shouldBe "NEG"

      probandClinicalImpression.getInvestigation.size() shouldBe 1
      probandClinicalImpression.getInvestigationFirstRep.getCode.getText shouldBe "Examination / signs"
      probandClinicalImpression.getInvestigationFirstRep.getItem.size() shouldBe 1
      probandClinicalImpression.getInvestigationFirstRep.getItemFirstRep.getReference shouldBe id(probandDiseaseStatus)

      mthClinicalImpression.getInvestigation.size() shouldBe 1
      mthClinicalImpression.getInvestigationFirstRep.getCode.getText shouldBe "Examination / signs"
      mthClinicalImpression.getInvestigationFirstRep.getItem.size() shouldBe 1
      mthClinicalImpression.getInvestigationFirstRep.getItemFirstRep.getReference shouldBe id(mthDiseaseStatus)

      analysisServiceRequest.getSupportingInfo.size() shouldBe 2
      analysisServiceRequest.getSupportingInfo.asScala.map(_.getReference) should contain theSameElementsAs Seq(id(probandClinicalImpression),id(mthClinicalImpression))

      //Validate documents that has been copied
      val bundleJson = s"$reportPath/bundle.json"
      assert(S3Utils.exists(inputBucket, bundleJson), s"Bundle json file $bundleJson does not exist")
      val fileCSV = s"$reportPath/files.csv"
      assert(S3Utils.exists(inputBucket, fileCSV), s"File CSV $fileCSV does not exist")
    }


  }


}