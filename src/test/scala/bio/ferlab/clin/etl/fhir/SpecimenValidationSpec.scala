package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils.{defaultAnalysis, defaultPatient}
import bio.ferlab.clin.etl.fhir.testutils.{FhirTestUtils, WithFhirServer}
import bio.ferlab.clin.etl.model
import bio.ferlab.clin.etl.model.TExistingSpecimen
import bio.ferlab.clin.etl.task.SpecimenValidation.{SampleType, validateOneSpecimen, validateSample, validateSpecimen}
import cats.data.Validated.Invalid
import cats.data._
import cats.implicits.catsSyntaxValidatedId
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.Enumerations.FHIRAllTypes
import org.hl7.fhir.r4.model.{Bundle, IdType, Specimen}
import org.scalatest.{FlatSpec, Matchers}

import java.io
import collection.JavaConverters._
import scala.collection.mutable

class SpecimenValidationSpec extends FlatSpec with Matchers with WithFhirServer {

  "validate samples" should "return a list of errors if samples are not associated to anothers donors" in {
    import bio.ferlab.clin.etl.fhir.testutils.MetadataTestUtils._

    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val ptId2 = FhirTestUtils.loadPatients().getIdPart
    val spId1 = FhirTestUtils.loadSpecimens2(ptId1, "CHUSJ", "1")
    val spId3 = FhirTestUtils.loadSpecimens2(ptId1, "CHUM", "1")
    val spId2 = FhirTestUtils.loadSpecimens2(ptId2, "CHUSJ", "2")
    val metadata = defaultMetadata.copy(
      analyses = Seq(
        defaultAnalysis.copy(specimenId = "1", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1")),
        defaultAnalysis.copy(specimenId = "2", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_2"))
      )
    )
    val analysis = defaultAnalysis.copy(specimenId = "1", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSpecimen(analysis) shouldBe Invalid(
      NonEmptyList.of(
        s"Specimen id=1 : does not belong to the same patient (not_the_good_one_1 <-> $ptId1)"
      )
    )


  }

  it should "return a corresponding map of sapmple ids if samples are associated to right donors" in {


  }

  "test" should "deede" in {
    import ca.uhn.fhir.rest.param.DateRangeParam
    import org.hl7.fhir.r4.model.Patient
    import org.hl7.fhir.r4.model.Provenance
    val ptId1 = FhirTestUtils.loadPatients().getIdPart
    val parent = FhirTestUtils.loadSpecimens2(ptId1, "CHUSJ", "1")
    val sample = FhirTestUtils.loadSpecimens2(ptId1, "CHUSJ", "2", parent = Some(parent))
    val analysis = defaultAnalysis.copy(specimenId = "1", sampleId="2", ldm = "CHUSJ", patient = defaultPatient("not_the_good_one_1"))
    validateSample(analysis)
  }
}
