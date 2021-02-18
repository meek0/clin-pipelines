package bio.ferlab.clin.etl.fhir.testutils

trait WholeStack extends LocalStackT with FhirServer

trait WithWholeStackSuite extends LocalStackT with FhirServerSuite

object StartWholeStack extends App with LocalStackT with FhirServer {
  println("Whole stack is started")
  while (true) {

  }
}




