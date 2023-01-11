package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.scripts.SwitchSpecimenValues

object Scripts extends App {
  withSystemExit {
    withLog {
      withConf { conf =>
        withExceptions {
          val (_, client) = buildFhirClients(conf.fhir, conf.keycloak)
          val script = if (args.length > 0) args.head else "";
          script match {
            // bellow all the scripts we can execute by name
            case "SwitchSpecimenValues" => SwitchSpecimenValues(client)
            case s: String => throw new IllegalArgumentException(s"unknown script: $s")
          }
        }
      }
    }
  }
}
