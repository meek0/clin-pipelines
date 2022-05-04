package bio.ferlab.clin.etl.task.fileimport

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.task.fileimport.model._
import ca.uhn.fhir.rest.client.api.IGenericClient
import org.slf4j.{Logger, LoggerFactory}

object BuildBundle {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def validate(metadata: Metadata, files: Seq[FileEntry])(implicit clinClient: IClinFhirClient, fhirClient: IGenericClient, ferloadConf: FerloadConf): ValidationResult[TBundle] = {
    metadata match {
      case s: SimpleMetadata => SimpleBuildBundle.validate(s, files)
      case f: FullMetadata => FullBuildBundle.validate(f, files)
    }
  }


}
