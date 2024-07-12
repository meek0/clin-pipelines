package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.FileImport.args
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.scripts.{AddGroupIdentifierTask, DeleteBatchByDate, FixBatchFiles, FixFerloadURLs, MigrateMetadata, MigrateServiceRequest, SwitchSpecimenValues}

object Scripts extends App {
  withSystemExit {
    withLog {
      withConf { conf =>
        withExceptions {
          val (_, fhirClient) = buildFhirClients(conf.fhir, conf.keycloak)
          val s3Client = buildS3Client(conf.aws)
          val script = if (args.length > 0) args.head else ""
          val params = if (args.length > 1) args.tail.map(_.toString) else Array.empty[String]
          script match {
            // bellow all the scripts we can execute by name
            case "SwitchSpecimenValues" => SwitchSpecimenValues(fhirClient, params)
            case "MigrateMetadata" => MigrateMetadata(params.head, conf)
            case "MigrateServiceRequest" => MigrateServiceRequest(fhirClient, params)
            case "AddGroupIdentifierTask" => AddGroupIdentifierTask(fhirClient, params)
            case "DeleteBatchByDate" => DeleteBatchByDate(fhirClient, params)
            case "FixBatchFiles" => FixBatchFiles(conf,params)(fhirClient, s3Client)
            case "FixFerloadURLs" => FixFerloadURLs(conf,params)(fhirClient)
            case s: String => throw new IllegalArgumentException(s"unknown script: $s")
          }
        }
      }
    }
  }
}
