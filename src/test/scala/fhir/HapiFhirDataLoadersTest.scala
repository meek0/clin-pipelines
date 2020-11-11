package fhir

import java.time.Duration

import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, ForAllTestContainer}
import org.hl7.fhir.r4.model.{DocumentManifest, DocumentReference, Specimen}
import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.wait.strategy.Wait

class HapiFhirDataLoadersTest extends FreeSpec with Matchers with BeforeAndAfterAll with ForAllTestContainer {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  val exposedPort:Int = 18080

  val fhirEnv: Map[String, String] = Map(
    "HAPI_DATASOURCE_URL" -> "jdbc:h2:mem:hapi",
    "HAPI_DATASOURCE_DRIVER" -> "org.h2.Driver",
    "HAPI_DATASOURCE_USERNAME" -> "",
    "HAPI_DATASOURCE_PASSWORD" -> "",
    "HAPI_AUTH_ENABLED" -> "false",
    "HAPI_BIO_ELASTICSEARCH_ENABLED" -> "false",
    "HAPI_LOGGING_INTERCEPTOR_SERVER_ENABLED" -> "false",
    "HAPI_LOGGING_INTERCEPTOR_CLIENT_ENABLED" -> "false",
    "HAPI_SERVER_ADDRESS" -> "http://localhost:18080/fhir/",
    "JAVA_OPTS" -> "-Dhibernate.dialect=org.hibernate.dialect.H2Dialect"
  )

  val container = FixedHostPortGenericContainer("chusj/clin-fhir-server:972a893",
    waitStrategy = Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(60)),
    exposedHostPort = exposedPort,
    exposedContainerPort = 8080,
    env = fhirEnv
  )

  val testUtil: FhirTestUtils = new FhirTestUtils(s"http://localhost:${exposedPort}/fhir")

  override def beforeAll() = {
    LOGGER.info("Preparing test.  Loading referential data.")
    testUtil.loadOrganizations()
    testUtil.loadPractitioners()
    testUtil.loadPatients()
  }

  override def afterAll() = {
    println("Shutting down.")
  }

  var specimens: Seq[Specimen] = Seq.empty
  var files:Seq[DocumentReference] = Seq.empty

  "Specimen and samples" - {
    "Specimen" in {
      specimens = testUtil.loadSpecimens()
      specimens.isEmpty shouldBe false

      // Search Hapi Fhir for a specimen we just created
      val specimen: Option[Specimen] = testUtil.findById(specimens.head.getId, classOf[Specimen])

      specimen.isDefined shouldBe true
      specimen.get.getId should not be empty
    }
    "Samples" in {
      val samples: Seq[Specimen] = testUtil.loadSamples(specimens.map(s => s.getId -> s)(collection.breakOut))
      samples.isEmpty shouldBe false

      // Search Hapi Fhir for a sample we just created
      val sample: Option[Specimen] = testUtil.findById(samples.head.getId, classOf[Specimen])

      sample.isDefined shouldBe true
      sample.get.getId should not be empty
    }
  }

  /*
   * COMMENTED OUT UNTIL WE FIGURE OUT A WAY TO LOAD THE IMPLEMENTATION GUIDE
   * IN HAPI FHIR UPON SERVER INITIALIZATION.
   *
  "Files" - {
    "Linked CRAM and CRAI" in {
      files = testUtil.loadFiles(specimens)
      // files.foreach(f => testUtil.printJson(f))

      val file: Option[DocumentReference] = testUtil.findById(files.head.getId, classOf[DocumentReference])
      file.isDefined shouldBe true

      file.get.getId should not be empty
    }
  }

  "Analysis" - {
    "Seuqencing Alignment" in {
      val analyses: Seq[DocumentManifest] = testUtil.loadAnalyses(files)

      val analysis: Option[DocumentManifest] = testUtil.findById(analyses.head.getId, classOf[DocumentManifest])
      analysis.isDefined shouldBe true

      analysis.get.getId should not be empty
    }
  }
   */
}