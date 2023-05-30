package bio.ferlab.clin.etl.task.fileimport

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.FhirUtils._
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.fileimport.model.FamilyExtension.buildFamilies
import bio.ferlab.clin.etl.task.fileimport.model.TFullServiceRequest.{EXTUM_SCHEMA, GERMLINE_SCHEMA}
import bio.ferlab.clin.etl.task.fileimport.model._
import bio.ferlab.clin.etl.task.fileimport.validation.DocumentReferencesValidation.validateFiles
import bio.ferlab.clin.etl.task.fileimport.validation.OrganizationValidation.{validateEpOrganization, validateLdmOrganization}
import bio.ferlab.clin.etl.task.fileimport.validation.SpecimenValidation
import bio.ferlab.clin.etl.task.fileimport.validation.SpecimenValidation.{validateSample, validateSpecimen}
import bio.ferlab.clin.etl.task.fileimport.validation.TaskExtensionValidation._
import bio.ferlab.clin.etl.task.fileimport.validation.full.AnalysisServiceRequestValidation.validateAnalysisServiceRequest
import bio.ferlab.clin.etl.task.fileimport.validation.full.ClinicalImpressionValidation.validateClinicalImpression
import bio.ferlab.clin.etl.task.fileimport.validation.full.ObservationValidation.validateObservation
import bio.ferlab.clin.etl.task.fileimport.validation.full.PatientValidation.validatePatient
import bio.ferlab.clin.etl.task.fileimport.validation.full.PersonValidation.validatePerson
import bio.ferlab.clin.etl.task.fileimport.validation.full.SequencingServiceRequestValidation.validateSequencingServiceRequest
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.ValidatedNel
import cats.implicits._
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import org.hl7.fhir.r4.model.{IdType, Resource}
import org.slf4j.{Logger, LoggerFactory}

object FullBuildBundle {


  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def validateSchema(submissionSchema: Option[String], metadata: Metadata): ValidatedNel[String, Option[String]] = {
    if (EXTUM_SCHEMA.equals(submissionSchema.orNull)) {
      if (metadata.analyses.flatMap(a => a.files.qc_metrics_tsv).isEmpty) {
        return "Submission schema of type EXTUM but no QC Metrics TSV files found".invalidNel
      }
    } else if (GERMLINE_SCHEMA.equals(submissionSchema.orNull)) {
      // could do some checks here
    } else {
      return s"Unsupported metadata schema, should be one of: [$GERMLINE_SCHEMA, $EXTUM_SCHEMA]".invalidNel
    }
    submissionSchema.validNel
  }

  def validate(metadata: FullMetadata, files: Seq[FileEntry])(implicit clinClient: IClinFhirClient, fhirClient: IGenericClient, ferloadConf: FerloadConf): ValidationResult[TBundle] = {
    LOGGER.info("################# Validate Resources ##################")

    val taskExtensions = validateTaskExtension(metadata)
    val mapFiles = files.map(f => (f.filename, f)).toMap
    val allResources: ValidatedNel[String, List[TemporaryBundle]] = metadata.analyses.toList.map { a =>

      val analysisServiceRequests: Option[ValidationResult[TAnalysisServiceRequest]] = if (a.patient.familyId.isEmpty || a.patient.familyMember == "PROBAND") {
        Some(validateAnalysisServiceRequest(metadata.submissionSchema, a))
      } else None

      val patient = validatePatient(a.patient)

      val specimen: ValidationResult[TSpecimen] = (patient, validateSpecimen(a)).mapN((_, _)).andThen { case (p, sp) => SpecimenValidation.validateFullPatient(sp, p, a.ldmSpecimenId, SpecimenType) }
      val sample: ValidationResult[TSpecimen] = (patient, validateSample(a)).mapN((_, _)).andThen { case (p, sa) => SpecimenValidation.validateFullPatient(sa, p, a.ldmSampleId, SampleType) }
      (
        validateSchema(metadata.submissionSchema, metadata),
        validateLdmOrganization(a),
        validateEpOrganization(a),
        patient,
        validatePerson(a.patient),
        validateClinicalImpression(a),
        analysisServiceRequests.sequence,
        validateSequencingServiceRequest(metadata.submissionSchema, a),
        specimen,
        sample,
        validateFiles(mapFiles, a),
        taskExtensions.map(_.forAliquot(a.labAliquotId)),
        validateObservation(a)
        ).mapN(TemporaryBundle.apply)

    }.sequence

    val bundleAndFamilies = groupResourcesByFamily(allResources)

    val entriesComponent: ValidatedNel[String, List[BundleEntryComponent]] = bundleAndFamilies.map { case (bundles, familyMap, analysisServiceRequestByFamily, clinicalImpressionsByFamily) =>
      bundles.flatMap(b => createResources(familyMap, b, analysisServiceRequestByFamily, clinicalImpressionsByFamily))
    }

    entriesComponent.map(TBundle)
  }

  private def groupResourcesByFamily(allResources: ValidatedNel[String, List[TemporaryBundle]]) = {
    allResources
      .map { bundles =>
        val families: Map[String, Seq[FamilyExtension]] = buildFamilies(bundles.map(_.patient))
        val analysisServiceRequestByFamily: Map[String, IdType] = bundles.flatMap(_.analysisServiceRequest)
          .collect { case a if a.analysis.patient.familyId.isDefined => (a.analysis.patient.familyId.get, a.id) }
          .groupBy { case (key, _) => key }.mapValues(l => l.map { case (_, value) => value }.head)
        val clinicalImpressionsByFamily: Map[String, Seq[IdType]] = bundles.map(_.clinicalImpression)
          .collect { case ci if ci.analysis.patient.familyId.isDefined => (ci.analysis.patient.familyId.get, ci.id) }
          .groupBy { case (key, _) => key }.mapValues(l => l.map { case (_, value) => value })

        (bundles, families, analysisServiceRequestByFamily, clinicalImpressionsByFamily)
      }
  }

  case class TemporaryBundle(submissionSchema: Option[String], ldm: IdType, ep: IdType, patient: TPatient, person: TPerson,
                             clinicalImpression: TClinicalImpression,
                             analysisServiceRequest: Option[TAnalysisServiceRequest], sequencingServiceRequest: TSequencingServiceRequest,
                             specimen: TSpecimen, sample: TSpecimen,
                             files: TDocumentReferences, taskExtensions: TaskExtensions, diseaseStatus: TObservation)

  def createResources(familyMap: Map[String, Seq[FamilyExtension]], b: TemporaryBundle, analysisServiceRequestByFamily: Map[String, IdType], clinicalImpressionsByFamily: Map[String, Seq[IdType]])(implicit ferloadConf: FerloadConf): List[BundleEntryComponent] = {
    val patientResource = b.patient.buildResource(b.ep.toReference())
    val bundleFamilyExtension = b.patient.patient.familyId.flatMap(f => familyMap.get(f))
    val analyseServiceRequestResource = b.analysisServiceRequest.map { a =>
      val clinicalImpressionReferences = b.patient.patient.familyId
        .flatMap(f => clinicalImpressionsByFamily.get(f))
        .getOrElse(Seq(b.clinicalImpression.id))
      a.buildResource(patientResource.toReference(), bundleFamilyExtension, clinicalImpressionReferences.map(_.toReference()), b.ldm.toReference())
    }
    val task = TTask(b.submissionSchema, b.taskExtensions)
    val personResource = b.person.buildResource(patientResource.toReference())
    val sequencingServiceRequestReference = b.sequencingServiceRequest.id.toReference()
    val specimenResource = b.specimen.buildResource(patientResource.toReference(), sequencingServiceRequestReference, b.ldm.toReference())
    val sampleResource = b.sample.buildResource(patientResource.toReference(), sequencingServiceRequestReference, b.ldm.toReference(), Some(specimenResource.toReference()))
    val documentReferencesResources: DocumentReferencesResources = b.files.buildResources(patientResource.toReference(), b.ldm.toReference(), sampleResource.toReference())

    val familyAnalysisServiceRequest: Option[IdType] = b.patient.patient.familyId.flatMap(f => analysisServiceRequestByFamily.get(f))
    val analysisServiceRequestReference = b.analysisServiceRequest.map(_.id.toReference()).orElse(familyAnalysisServiceRequest.map(_.toReference()))
    val sequencingServiceRequestResource = b.sequencingServiceRequest.buildResource(analysisServiceRequestReference,
      patientResource.toReference(), specimenResource.toReference(), sampleResource.toReference(), b.ldm.toReference())

    val taskResource: Resource = task.buildResource(analysisServiceRequestReference, sequencingServiceRequestReference, patientResource.toReference(), b.ldm.toReference(), sampleResource.toReference(), documentReferencesResources)
    val clinicalImpressionResource = b.clinicalImpression.createResource(patientResource.toReference(), b.diseaseStatus.id.toReference())
    val observationResource = b.diseaseStatus.createResource(patientResource.toReference())

    val initResourcesToCreate = (documentReferencesResources.resources() :+ taskResource :+ sequencingServiceRequestResource :+ clinicalImpressionResource :+ observationResource).toList
    val afterASRToCreate = analyseServiceRequestResource.map(a => initResourcesToCreate :+ a).getOrElse(initResourcesToCreate)

    val (afterPatientToCreate, afterPatientToUpdate) = b.patient match {
      case _: TNewPatient => (afterASRToCreate :+ patientResource, Seq())
      case _: TExistingPatient => (afterASRToCreate, Seq() :+ patientResource)
    }
    val (resourcesToCreate, resourcesToUpdate) = b.person match {
      case _: TNewPerson => (afterPatientToCreate :+ personResource, afterPatientToUpdate)
      case _: TExistingPerson => (afterPatientToCreate, afterPatientToUpdate :+ personResource)
    }
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

    val bundleEntriesToCreate: Seq[BundleEntryComponent] = bundleEntriesSpecimen ++ bundleCreate(resourcesToCreate)

    val bundleEntriesToUpdate = bundleUpdate(resourcesToUpdate)
    (bundleEntriesToCreate ++ bundleEntriesToUpdate).toList


  }

}
