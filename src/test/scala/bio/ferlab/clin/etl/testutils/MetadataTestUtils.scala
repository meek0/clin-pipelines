package bio.ferlab.clin.etl.testutils

import MetadataTestUtils.defaultMetadata.analyses
import bio.ferlab.clin.etl.task.fileimport.model.TFullServiceRequest.{EXTUM_SCHEMA, GERMLINE_SCHEMA}
import bio.ferlab.clin.etl.task.fileimport.model._

object MetadataTestUtils {

  def defaultPatient(ptId1: String, firstName: String = "John", lastName: String = "Doe", sex: String = "male"): SimplePatient = {
    SimplePatient(ptId1, firstName, lastName, sex)
  }

  def defaultFullPatient(firstName: String = "John",
                         lastName: String = "Doe",
                         sex: String = "male",
                         ddn:String = "10/12/2000",
                         ramq: Option[String] = Some("DOWJ1234"),
                         ndm: Option[String] = None,
                         ep: String = "CHUSJ",
                         designFamily: Option[String] = Some("SOLO"),
                         position: String = "PROB",
                         familyId: Option[String] = None,
                         status: String = "AFF",
                         fetus:Option[Boolean] = None
                        ): FullPatient = {
    FullPatient(firstName = firstName, lastName = lastName, sex = sex, birthDate=ddn,
      ramq = ramq, mrn = ndm, ep = ep, designFamily = designFamily,
      familyMember = position, familyId = familyId, status = status, fetus = fetus)
  }

  val defaultExperiment: Experiment = Experiment(
    platform = Some("Illumina"),
    sequencerId = Some("NB552318"),
    runName = Some("runNameExample"),
    runDate = Some("2014-09-21T11:50:23-05:00"),
    runAlias = Some("runAliasExample"),
    flowcellId = Some("0"),
    isPairedEnd = Some(true),
    fragmentSize = Some(100),
    experimentalStrategy = Some("WXS"),
    captureKit = Some("RocheKapaHyperExome"),
    baitDefinition = Some("KAPA_HyperExome_hg38_capture_targets")
  )
  val defaultWorkflow: Workflow = Workflow(
    name = Some("Dragen"),
    version = Some("1.1.0"),
    genomeBuild = Some("GRCh38")
  )
  val defaultFullAnalysis: FullAnalysis = FullAnalysis(
    patient = defaultFullPatient(),
    ldm = "LDM-CHUSJ",
    ldmSampleId = "submitted_sample_id",
    ldmSpecimenId = "submitted_specimen_id",
    specimenType = "NBL",
    sampleType = Some("DNA"),
    ldmServiceRequestId = "clin_prescription_id",
    labAliquotId = "nanuq_sample_id",
    priority = None,
    panelCode = None,
    analysisCode = Some("MMG"),
    files = defaultFilesAnalysis,
    experiment = defaultExperiment,
    workflow = defaultWorkflow
  )

  val defaultFilesAnalysis: FilesAnalysis = FilesAnalysis(
    cram = "file1.cram",
    crai = "file1.cram.crai",
    snv_vcf = "file2.vcf.gz",
    snv_tbi = "file2.vcf.gz.tbi",
    cnv_vcf = "file4.vcf.gz",
    cnv_tbi = "file4.vcf.gz.tbi",
    sv_vcf = Some("file5.vcf.gz"),
    sv_tbi = Some("file5.vcf.gz.tbi"),
    supplement = "file3.tgz",
    exomiser_html = Some("file6.html"),
    exomiser_json = Some("file6.json"),
    exomiser_variants_tsv = Some("file6.variants.tsv"),
    seg_bw = Some("file7.seg.bw"),
    hard_filtered_baf_bw = Some("file7.baf.bw"),
    roh_bed = Some("file7.roh.bed"),
    hyper_exome_hg38_bed = Some("file7.exome.bed"),
    cnv_calls_png = Some("file8.png"),
    coverage_by_gene_csv = Some("file9.csv"),
    qc_metrics = Some("file10.json"),
  )
  val defaultFilesAnalysisWithOptionals: FilesAnalysis = FilesAnalysis(
    cram = "file1.cram",
    crai = "file1.cram.crai",
    snv_vcf = "file2.vcf.gz",
    snv_tbi = "file2.vcf.gz.tbi",
    cnv_vcf = "file4.vcf.gz",
    cnv_tbi = "file4.vcf.gz.tbi",
    sv_vcf = Some("file5.vcf.gz"),
    sv_tbi = Some("file5.vcf.gz.tbi"),
    supplement = "file3.tgz",
    exomiser_html = None,
    exomiser_json = None,
    exomiser_variants_tsv = None,
    seg_bw =  None,
    hard_filtered_baf_bw =  None,
    roh_bed =  None,
    hyper_exome_hg38_bed = None,
    cnv_calls_png = None,
    coverage_by_gene_csv = None,
    qc_metrics = None,
  )

  val defaultAnalysis: SimpleAnalysis = SimpleAnalysis(
    patient = defaultPatient("clin_id"),
    ldm = "CHUSJ",
    ldmSampleId = "submitted_sample_id",
    ldmSpecimenId = "submitted_specimen_id",
    specimenType = "NBL",
    sampleType = Some("DNA"),
    clinServiceRequestId = "clin_prescription_id",
    labAliquotId = "nanuq_sample_id",
    priority = None,
    files = defaultFilesAnalysis,
    experiment = defaultExperiment,
    workflow = defaultWorkflow
  )
  val defaultAnalysisWithOptionals: SimpleAnalysis = SimpleAnalysis(
    patient = defaultPatient("clin_id"),
    ldm = "CHUSJ",
    ldmSampleId = "submitted_sample_id",
    ldmSpecimenId = "submitted_specimen_id",
    specimenType = "NBL",
    sampleType = Some("DNA"),
    clinServiceRequestId = "clin_prescription_id",
    labAliquotId = "nanuq_sample_id",
    priority = None,
    files = defaultFilesAnalysisWithOptionals,
    experiment = defaultExperiment,
    workflow = defaultWorkflow
  )
  val defaultMetadata: SimpleMetadata = SimpleMetadata(
    Some(GERMLINE_SCHEMA),
    analyses = Seq(
      defaultAnalysis
    )
  )

  val defaultMetadataWithOptionals: SimpleMetadata = SimpleMetadata(
    Some(GERMLINE_SCHEMA),
    analyses = Seq(
      defaultAnalysisWithOptionals
    )
  )

  val extumMetadataWithOptionals: SimpleMetadata = SimpleMetadata(
    Some(EXTUM_SCHEMA),
    analyses = Seq(
      defaultAnalysisWithOptionals
    )
  )

  val extumMetadataInvalidQcMetrics: SimpleMetadata = SimpleMetadata(
    Some(EXTUM_SCHEMA),
    analyses = Seq(
      defaultAnalysis
    )
  )

  val unsupportedMetadataWithOptionals: SimpleMetadata = SimpleMetadata(
    None,
    analyses = Seq(
      defaultAnalysisWithOptionals
    )
  )
}