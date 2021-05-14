package bio.ferlab.clin.etl.fhir.testutils

import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.fhir.testutils.containers.FhirServerContainer
import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import org.scalatest.{BeforeAndAfterAll, TestSuite}

trait FhirServer {
  val fhirPort: Int = FhirServerContainer.startIfNotRunning()
  val fhirBaseUrl = s"http://localhost:$fhirPort/fhir"
  val fhirContext: FhirContext = FhirContext.forR4()
  fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
  fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)
  val parser: IParser = fhirContext.newJsonParser().setPrettyPrint(true)

  implicit val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], fhirBaseUrl)
  implicit val fhirClient: IGenericClient = fhirContext.newRestfulGenericClient(fhirBaseUrl)

}

trait FhirServerSuite extends FhirServer with TestSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    FhirTestUtils.init()
  }

  override def afterAll(): Unit = {
   FhirTestUtils.clearAll()
  }
}

object StartFhirServer extends App with FhirServer {
  println("Fhir Server is started")
  while (true) {

  }
}

object test extends FhirServer with App {
  FhirTestUtils.loadPatients(lastName = "River", firstName = "Jack")
}