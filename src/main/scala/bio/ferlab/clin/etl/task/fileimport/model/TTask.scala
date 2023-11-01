package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import org.hl7.fhir.r4.model.Task.{ParameterComponent, TaskOutputComponent}
import org.hl7.fhir.r4.model.{CodeableConcept, Extension, IdType, Identifier, Reference, Resource, StringType}
import TTask._
import bio.ferlab.clin.etl.fhir.FhirUtils.ResourceExtension
import bio.ferlab.clin.etl.task.fileimport.model.TFullServiceRequest.EXTUM_SCHEMA
import bio.ferlab.clin.etl.task.fileimport.validation.SpecimenValidation.CQGC_LAB

case class TaskExtensions(workflowExtension: Extension, experimentExtension: Extension) {
  def forAliquot(labAliquotId: String): TaskExtensions = {
    val expExtension = experimentExtension.copy()
    expExtension.addExtension(new Extension("labAliquotId", new StringType(labAliquotId)))
    this.copy(experimentExtension = expExtension)
  }
}

case class TTask(submissionSchema: Option[String], taskExtensions: TaskExtensions) {

  def buildResource(analysisRef: Option[Reference], sequencingRef: Reference, patient: Reference, requester: Reference, sample: Reference, drr: DocumentReferencesResources, batchId: String): Resource = {
    val t = AnalysisTask()

    if (EXTUM_SCHEMA.equals(submissionSchema.orNull)) {
      t.getCode.addCoding()
        .setSystem(CodingSystems.ANALYSIS_TYPE)
        .setCode(EXTUM_ANALYSIS)
    } else {
      t.getCode.addCoding()
        .setSystem(CodingSystems.ANALYSIS_TYPE)
        .setCode(EXOME_GERMLINE_ANALYSIS)
    }

    t.setFocus(sequencingRef)
    if (analysisRef.isDefined) {
      t.getBasedOn.add(analysisRef.get)
    }
    t.setFor(patient)

    t.setRequester(requester)
    t.setOwner(new Reference(s"Organization/$CQGC_LAB"))
    t.setGroupIdentifier(new Identifier().setValue(batchId))
    val input = new ParameterComponent()
    input.setType(new CodeableConcept().setText(ANALYSED_SAMPLE))
    input.setValue(sample)
    t.addInput(input)
    val sequencingExperimentOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(SequencingAlignment.documentType)
      val sequencingAlignment = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.sequencingAlignment.toReference())
      sequencingAlignment
    }

    val variantCallOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(VariantCalling.documentType)
      val variantCalling = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.variantCalling.toReference())
      variantCalling
    }

    val cnvOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(CopyNumberVariant.documentType)
      val cnv = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.copyNumberVariant.toReference())
      cnv
    }

    val supplementOutput = {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(SupplementDocument.documentType)
      val sup = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.supplement.toReference())
      sup
    }

    val structuralVariantOutput = if (drr.structuralVariant != null) {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(StructuralVariant.documentType)
      val sup = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.structuralVariant.toReference())
      sup
    } else null

    val exomiserOutput = if (drr.exomiser != null) {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(Exomiser.documentType)
      val sup = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.exomiser.toReference())
      sup
    } else null

    val igvTrackOutput = if (drr.igvTrack != null) {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(IgvTrack.documentType)
      val sup = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.igvTrack.toReference())
      sup
    } else null

    val cnvVisualizationOutput = if (drr.cnvVisualization != null) {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(CnvVisualization.documentType)
      val sup = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.cnvVisualization.toReference())
      sup
    } else null

    val coverageByGeneOutput = if (drr.coverageByGene != null) {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(CoverageByGene.documentType)
      val sup = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.coverageByGene.toReference())
      sup
    } else null

    val qcMetricsOutput = if (drr.qcMetrics != null) {
      val code = new CodeableConcept()
      code.addCoding()
        .setSystem(CodingSystems.DR_TYPE)
        .setCode(QcMetrics.documentType)
      val sup = new TaskOutputComponent()
        .setType(code)
        .setValue(drr.qcMetrics.toReference())
      sup
    } else null

    Seq(sequencingExperimentOutput, variantCallOutput, cnvOutput, structuralVariantOutput, supplementOutput, exomiserOutput, igvTrackOutput, cnvVisualizationOutput, coverageByGeneOutput, qcMetricsOutput)
      .filter(_ != null)
      .foreach { r =>
        t.addOutput(r)
      }

    t.setId(IdType.newRandomUuid())
    t.addExtension(taskExtensions.workflowExtension)
    t.addExtension(taskExtensions.experimentExtension)
    t
  }
}

object TTask {
  val EXOME_GERMLINE_ANALYSIS = "GEBA"
  val EXTUM_ANALYSIS = "TEBA"

  val ANALYSED_SAMPLE = "Analysed sample"
}
