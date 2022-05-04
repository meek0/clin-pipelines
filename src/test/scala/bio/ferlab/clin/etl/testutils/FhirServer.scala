package bio.ferlab.clin.etl.testutils

import bio.ferlab.clin.etl.conf.FerloadConf
import bio.ferlab.clin.etl.fhir.IClinFhirClient
import bio.ferlab.clin.etl.testutils.containers.FhirServerContainer
import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.api.SummaryEnum
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import org.hl7.fhir.instance.model.api.IBaseResource
import org.hl7.fhir.r4.model.{Bundle, IdType, Resource}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, TestSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Random

trait FhirServer {
  val (fhirPort, isNew) = FhirServerContainer.startIfNotRunning()
  val fhirBaseUrl = s"http://localhost:$fhirPort/fhir"
  val fhirContext: FhirContext = FhirContext.forR4()
  fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
  fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)
  val parser: IParser = fhirContext.newJsonParser().setPrettyPrint(true)

  implicit val clinClient: IClinFhirClient = fhirContext.newRestfulClient(classOf[IClinFhirClient], fhirBaseUrl)
  implicit val fhirClient: IGenericClient = fhirContext.newRestfulGenericClient(fhirBaseUrl)
  if(isNew) {
    FhirServerContainer.init()
  }

}

trait FhirServerSuite extends FhirServer with TestSuite with BeforeAndAfterAll with BeforeAndAfter {
  implicit val ferloadConf: FerloadConf = FerloadConf("https://objectstore.cqgc.qc.ca")

  override def beforeAll(): Unit = {
    FhirTestUtils.clearAll()
  }

  def nextId(): String = java.util.UUID.randomUUID.toString

  def read[T <: IBaseResource](b: Bundle, theClass: Class[T]): Seq[T] = {

    b.getEntry.asScala.map { be =>

      val resourceId = id(be.getResource)
//      println(s"Read resource $resourceId")
      val t: T = fhirClient.read().resource(theClass).withId(resourceId).execute()
//      println(parser.encodeResourceToString(t))
      t
    }
  }

  def id(r: Resource): String = {
    IdType.of(r).toUnqualifiedVersionless.toString
  }

  def searchFhir(resourceType: String): Bundle = {
    fhirClient.search().forResource(resourceType)
      .returnBundle(classOf[Bundle])
      .summaryMode(SummaryEnum.TRUE)
      .execute()
  }
}

object StartFhirServer extends App with FhirServer {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  LOGGER.info("Fhir Server is started")
  while (true) {

  }
}

object test extends FhirServer with App {
  FhirTestUtils.loadPatients(lastName = "River", firstName = "Jack")
}