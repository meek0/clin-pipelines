package bio.ferlab.clin.etl.task.fileimport

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.FhirUtils._
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.fileimport.model._
import bio.ferlab.clin.etl.task.fileimport.validation.DocumentReferencesValidation.validateFiles
import bio.ferlab.clin.etl.task.fileimport.validation.OrganizationValidation.{validateCQGCOrganization, validateOrganization}
import bio.ferlab.clin.etl.task.fileimport.validation.PatientValidation.validatePatient
import bio.ferlab.clin.etl.task.fileimport.validation.ServiceRequestValidation.validateServiceRequest
import bio.ferlab.clin.etl.task.fileimport.validation.SpecimenValidation.{validateAliquot, validateSample, validateSpecimen}
import bio.ferlab.clin.etl.task.fileimport.validation.TaskExtensionValidation._
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.hl7.fhir.r4.model.{IdType, Resource}
import org.slf4j.{Logger, LoggerFactory}

object BuildBundle {


  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def validate(metadata: Metadata, files: Seq[FileEntry])(implicit clinClient: IClinFhirClient, fhirClient: IGenericClient, ferloadConf: FerloadConf): ValidationResult[TBundle] = {
    LOGGER.info("################# Validate Resources ##################")
    val taskExtensions = validateTaskExtension(metadata)
    val cqgcOrg = validateCQGCOrganization()
    val mapFiles = files.map(f => (f.filename, f)).toMap
    val allResources: ValidatedNel[String, List[BundleEntryComponent]] = metadata.analyses.toList.map { a =>

      (
        validateOrganization(a),
        validatePatient(a.patient),
        validateServiceRequest(a),
        validateSpecimen(a),
        validateSample(a),
        validateAliquot(a),
        validateFiles(mapFiles, a),
        taskExtensions,
        cqgcOrg
        ).mapN(createResources)

    }.combineAll

    allResources.map(TBundle)
  }

  def createResources(organization: IdType, patient: IdType, serviceRequest: TServiceRequest, specimen: TSpecimen, sample: TSpecimen, aliquot: TSpecimen, files: TDocumentReferences, taskExtensions: TaskExtensions, cqgcOrg: IdType)(implicit ferloadConf: FerloadConf): List[BundleEntryComponent] = {
    val task = TTask(taskExtensions)
    val specimenResource = specimen.buildResource(patient.toReference(), serviceRequest.sr.toReference(), organization.toReference())
    val sampleResource = sample.buildResource(patient.toReference(), serviceRequest.sr.toReference(), organization.toReference(), Some(specimenResource.toReference()))
    val aliquotResource = aliquot.buildResource(patient.toReference(), serviceRequest.sr.toReference(), cqgcOrg.toReference(), Some(sampleResource.toReference()))
    val documentReferencesResources: DocumentReferencesResources = files.buildResources(patient.toReference(), organization.toReference(), aliquotResource.toReference())
    val serviceRequestResource = serviceRequest.buildResource(specimenResource.toReference(), sampleResource.toReference(), aliquotResource.toReference())
    val taskResource: Resource = task.buildResource(serviceRequest.sr.toReference(), patient.toReference(), organization.toReference(), aliquotResource.toReference(), documentReferencesResources)

    val resourcesToCreate = (documentReferencesResources.resources() :+ taskResource).toList

    val resourcesToUpdate = Seq(serviceRequestResource).flatten

    val bundleEntriesSpecimen = Seq(specimenResource.toOption, sampleResource.toOption, aliquotResource.toOption).flatten.map { s =>
      val be = new BundleEntryComponent()
      be.setFullUrl(s.getIdElement.getValue)
        .setResource(s)
        .getRequest
        .setUrl("Specimen")
        .setIfNoneExist(s"accession=${s.getAccessionIdentifier.getSystem}|${s.getAccessionIdentifier.getValue}")
        .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.POST)
      be
    }

    val bundleEntriesToCreate: Seq[BundleEntryComponent] = bundleEntriesSpecimen ++ bundleCreate(resourcesToCreate)

    val bundleEntriesToUpdate = bundleUpdate(resourcesToUpdate)
    (bundleEntriesToCreate ++ bundleEntriesToUpdate).toList


  }

}
