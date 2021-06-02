package bio.ferlab.clin.etl.task.ingestion

import bio.ferlab.clin.etl.task.Conf
import cats.data.Validated.Invalid
import cats.data.{NonEmptyList, Validated}

import scala.concurrent.duration.DurationInt

object Exporter extends App {

  val result: Validated[NonEmptyList[String], Unit] = Conf.readConf().map { conf =>
    val Array(tables) = args
    val fhirExporter = new FhirBulkExporter(
      conf.keycloak, conf.fhir.url, conf.aws
    )

    val url = tables match {
      case "all" => fhirExporter.requestBulkExportFor(FhirBulkExporter.ALL_ENTITIES)
      case entities => fhirExporter.requestBulkExportFor(entities)
    }

    val files = fhirExporter.checkPollingStatus(url, 1.seconds, 100.seconds)
    fhirExporter.uploadFiles(files)
  }
  result match {
    case Invalid(NonEmptyList(h, t)) =>
      println(h)
      t.foreach(println)
      System.exit(-1)
    case Validated.Valid(_) => println("Success!")
  }
}

