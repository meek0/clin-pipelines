package bio.ferlab.clin.etl.task.ldmnotifier.model

case class ManifestRow(
                        url: String,
                        fileName: String,
                        fileType: String,
                        fileFormat: String,
                        hash: Option[String],
                        ldmSampleId: String,
                        patientId: String,
                        serviceRequestId: String,
                        cqgcLink: String
                      )