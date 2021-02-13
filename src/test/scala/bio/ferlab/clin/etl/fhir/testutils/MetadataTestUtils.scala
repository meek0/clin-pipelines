package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.model._

object MetadataTestUtils {

  def defaultPatient(ptId1: String, firstName: String = "John", lastName: String = "Doe", sex: String = "male"): InputPatient = {
    InputPatient(ptId1, firstName, lastName, sex)
  }

  val defaultAnalysis: Analysis = Analysis(
    patient = defaultPatient("clin_id"),
    ldm = "CHUSJ",
    sampleId = "submitted_sample_id",
    specimenId = "submitted_specimen_id",
    specimenType = "BLD",
    sampleType = Some("DNA"),
    bodySite = "can be null",
    serviceRequestId = "clin_prescription_id",
    labAliquotId = Some("nanuq_sample_id"),
    files = FilesAnalysis(
      cram = "file1.cram",
      crai = "file1.crai",
      vcf = "file2.vcf",
      tbi = "file2.tbi",
      qc = "file3.tgz"
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
