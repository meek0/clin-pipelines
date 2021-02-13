package bio.ferlab.clin.etl.task

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.fhir.IClinFhirClient.opt
import bio.ferlab.clin.etl.model._
import bio.ferlab.clin.etl.task.PatientValidation.validatePatient
import bio.ferlab.clin.etl.task.SpecimenValidation.{validateSample, validateSpecimen}
import bio.ferlab.clin.etl.{EitherResourceExtension, IdTypeExtension, ResourceExtension, ValidationResult}
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.param.StringParam
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.hl7.fhir.r4.model.IdType

object BuildBundle {

  def validate(metadata: Metadata, files: Seq[FileEntry])(implicit clinClient: IClinFhirClient, fhirClient: IGenericClient): ValidationResult[Bundle] = {
    val mapFiles = files.map(f => (f.name, f)).toMap
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

    allResources.map(Bundle)


  }

  def createResources(m: Metadata)(organization: IdType, patient: IdType, serviceRequest: ServiceRequest, specimen: Specimen, sample: Specimen, files: DocumentReferences, tasks: Tasks): List[BundleEntryComponent] = {
    val specimenResource = specimen.buildResource(patient.toReference(), serviceRequest.sr.toReference())
    val sampleResource = sample.buildResource(patient.toReference(), serviceRequest.sr.toReference(), Some(specimenResource.toReference()))
    val documentReferencesResources = files.buildResources(patient.toReference(), organization.toReference(), sampleResource.toReference())

    val resourcesToCreate = (Seq(specimenResource.toOption, sampleResource.toOption).flatten ++ documentReferencesResources.resources()).toList

    val resourcesToUpdate = Seq(serviceRequest.buildResource(specimenResource.toReference(), sampleResource.toReference()))

    val bundleEntriesToCreate = resourcesToCreate.map { fhirResource =>
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
      be.setFullUrl(fhirResource.getIdElement().getIdPart)
        .setResource(fhirResource)
        .getRequest
        .setUrl(fhirResource.getIdElement().getValue())
        .setMethod(org.hl7.fhir.r4.model.Bundle.HTTPVerb.PUT)
      be
    }
    bundleEntriesToCreate ++ bundleEntriesToUpdate


  }

  def validateServiceRequest(a: Analysis)(implicit client: IClinFhirClient): ValidatedNel[String, ServiceRequest] = {
    val fhirServiceRequest = opt(client.getServiceRequestById(new IdType(a.serviceRequestId)))
    fhirServiceRequest match {
      case None => s"ServiceRequest ${a.serviceRequestId} does not exist".invalidNel[ServiceRequest]
      case Some(fsr) => ServiceRequest(fsr).validNel[String]
    }
  }

  def validateOrganization(a: Analysis)(implicit client: IClinFhirClient): ValidatedNel[String, IdType] = {
    val fhirOrg = opt(client.findByNameOrAlias(new StringParam(a.ldm)))
    fhirOrg match {
      case None => s"Organization ${a.ldm} does not exist".invalidNel[IdType]
      case Some(org) => IdType.of(org).validNel[String]
    }
  }

  def validateFiles(files: Map[String, FileEntry], a: Analysis): ValidationResult[DocumentReferences] = {

    def validateOneFile(f: String) = files.get(f) match {
      case Some(file) => DocumentReference(objectStoreId = file.id, title = f, md5 = file.md5).validNel[String]
      case None => s"File ${f} does not exist".invalidNel[DocumentReference]
    }

    (
      validateOneFile(a.files.cram),
      validateOneFile(a.files.crai),
      validateOneFile(a.files.vcf),
      validateOneFile(a.files.tbi),
      validateOneFile(a.files.qc)
      ).mapN(DocumentReferences)

  }

  def validateTasks(a: Analysis): ValidationResult[Tasks] = Tasks(Task(), Task(), Task()).validNel[String]

}
