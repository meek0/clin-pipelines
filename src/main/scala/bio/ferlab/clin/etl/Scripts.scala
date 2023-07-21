package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.FileImport.args
import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.scripts.{MigrateMetadata, MigrateServiceRequest, SwitchSpecimenValues}

object Scripts extends App {
  withSystemExit {
    withLog {
      withConf { conf =>
        withExceptions {
          val (_, client) = buildFhirClients(conf.fhir, conf.keycloak)
          val script = if (args.length > 0) args.head else ""
          val params = if (args.length > 1) args.tail.map(_.toString) else Array.empty[String]
          script match {
            // bellow all the scripts we can execute by name
            case "SwitchSpecimenValues" => SwitchSpecimenValues(client, params)
            case "MigrateMetadata" => MigrateMetadata(params.head, conf)
            case "MigrateServiceRequest" => MigrateServiceRequest(client, params)
            case s: String => throw new IllegalArgumentException(s"unknown script: $s")
          }
        }
      }
    }
  }
}
