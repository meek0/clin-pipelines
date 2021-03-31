package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.fhir.FhirUtils.{EitherResourceExtension, IdTypeExtension, ResourceExtension}
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.model._
import bio.ferlab.clin.etl.task.validation.DocumentReferencesValidation.validateFiles
import bio.ferlab.clin.etl.task.validation.OrganizationValidation.validateOrganization
import bio.ferlab.clin.etl.task.validation.PatientValidation.validatePatient
import bio.ferlab.clin.etl.task.validation.ServiceRequestValidation.validateServiceRequest
import bio.ferlab.clin.etl.task.validation.SpecimenValidation.{validateSample, validateSpecimen}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.implicits._
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.hl7.fhir.r4.model.IdType

object BuildBundle {

  def validate(metadata: Metadata, files: Seq[FileEntry])(implicit clinClient: IClinFhirClient, fhirClient: IGenericClient): ValidationResult[TBundle] = {
    println("################# Validate Resources ##################")
    val mapFiles = files.map(f => (f.filename, f)).toMap
    val allResources = metadata.analyses.toList.map { a =>

      (
        validateOrganization(a),
        validatePatient(a.patient),
        validateServiceRequest(a),
        validateSpecimen(a),
        validateSample(a),
        validateFiles(mapFiles, a),
        validateTasks(a),
        ).mapN(createResources(metadata))

    }.combineAll

    allResources.map(TBundle)
  }

  def createResources(m: Metadata)(organization: IdType, patient: IdType, serviceRequest: TServiceRequest, specimen: TSpecimen, sample: TSpecimen, files: TDocumentReferences, tasks: TTasks): List[BundleEntryComponent] = {
    val specimenResource = specimen.buildResource(patient.toReference(), serviceRequest.sr.toReference())
    val sampleResource = sample.buildResource(patient.toReference(), serviceRequest.sr.toReference(), Some(specimenResource.toReference()))
    val documentReferencesResources: DocumentReferencesResources = files.buildResources(patient.toReference(), organization.toReference(), sampleResource.toReference())
    val serviceRequestResource = serviceRequest.buildResource(specimenResource.toReference(), sampleResource.toReference())
    val taskResources = tasks.buildResources(serviceRequest.sr.toReference(), patient.toReference(), organization.toReference(), sampleResource.toReference(), documentReferencesResources)

    val resourcesToCreate = (
      documentReferencesResources.resources()
        ++ taskResources
      ).toList

    val resourcesToUpdate = Seq(serviceRequestResource).flatten

    val bundleEntriesSpecimen = Seq(specimenResource.toOption, sampleResource.toOption).flatten.map { s =>
      val be = new BundleEntryComponent()
      be.setFullUrl(s.getIdElement.getValue)
        .setResource(s)
        .getRequest
        .setUrl("Specimen")
        .setIfNoneExist(s"accession=${s.getAccessionIdentifier.getSystem}|${s.getAccessionIdentifier.getValue}")
        .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.POST)
      be
    }

    val bundleEntriesToCreate: Seq[BundleEntryComponent] = bundleEntriesSpecimen ++ resourcesToCreate.map { fhirResource =>
      val be = new BundleEntryComponent()
      be.setFullUrl(fhirResource.getIdElement.getValue)
        .setResource(fhirResource)
        .getRequest
        .setUrl(fhirResource.fhirType())
        .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.POST)
      be
    }

    val bundleEntriesToUpdate = resourcesToUpdate.map { fhirResource =>
      val be = new BundleEntryComponent()
      be.setFullUrl(fhirResource.getIdElement.getIdPart)
        .setResource(fhirResource)
        .getRequest
        .setUrl(fhirResource.getIdElement.getValue)
        .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.PUT)
      be
    }
    (bundleEntriesToCreate ++ bundleEntriesToUpdate).toList


  }


  def validateTasks(a: Analysis): ValidationResult[TTasks] = TTasks(TTask(), TTask(), TTask()).validNel[String]

}
