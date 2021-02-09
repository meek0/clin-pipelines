package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.model._

object MetadataTestUtils {

  def defaultPatient(ptId1: String, firstName: String = "John", lastName: String = "Doe", sex: String = "male"): Patient = {
    Patient(ptId1, firstName, lastName, sex)
  }

  val defaultAnalysis: Analysis = Analysis(
    patient = defaultPatient("clin_id"),
    ldm = "ICM",
    sampleId = "submitted_sample_id",
    specimenId = "submitted_specimen_id",
    specimenType = Some("Normal blood"),
    sampleType = Some("DNA"),
    bodySite = Some("can be null"),
    serviceRequestId = "clin_prescription_id",
    labAliquotId = Some("nanuq_sample_id"),
    files = FileAnalyses(
      SA = Seq(FileAnalysis("file1.cram", "CRAM")),
      VC = Seq(FileAnalysis("file1.vcf", "VCF")),
      QC = Seq(FileAnalysis("file1.json", "QC"))
    )

  )
  val defaultMetadata: Metadata = Metadata(
    Experiment(
      platform = Some("Illumina"),
      sequencerId = Some("NB552318"),
      runName = Some("runNameExample"),
      runDate = Some("2014-09-21T11:50:23-05"),
      runAlias = Some("runAliasExample"),
      flowcellId = Some("0"),
      isPairedEnd = Some(true),
      fragmentSize = Some(100),
      experimentalStrategy = Some("WXS"),
      captureKit = Some("RocheKapaHyperExome"),
      baitDefinition = Some("KAPA_HyperExome_hg38_capture_targets")
    ),
    Workflow(
      name = Some("Dragen"),
      version = Some("1.1.0"),
      genomeBuild = Some("GRCh38")
    ),
    analyses = Seq(
      defaultAnalysis
    )

  )

}
