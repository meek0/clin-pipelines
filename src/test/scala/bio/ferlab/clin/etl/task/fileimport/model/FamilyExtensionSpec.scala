package bio.ferlab.clin.etl.task.fileimport.model

import bio.ferlab.clin.etl.testutils.MetadataTestUtils.defaultFullPatient
import bio.ferlab.clin.etl.testutils.{MetadataTestUtils, MinioServerSuite}
import org.hl7.fhir.r4.model.{IdType, Patient}
import org.scalatest.{FlatSpec, Matchers}

class FamilyExtensionSpec extends FlatSpec with MinioServerSuite with Matchers {
  case class MockPatient(id: IdType, patient: FullPatient) extends TPatient {
    override def resourcePatient: Patient = null
  }

  object MockPatient {
    def apply(id: String, patient: FullPatient): MockPatient = {
      val idType = new IdType()
      idType.setValue(id)
      new MockPatient(idType, patient)
    }
  }

  "buildFamilies" should "return families " in {

    val patients = Seq(
      MockPatient("1", defaultFullPatient(familyId = Some("1"), position = "PROBAND")),
      MockPatient("2", defaultFullPatient(familyId = Some("1"), position = "MTH")),
      MockPatient("3", defaultFullPatient(familyId = Some("1"), position = "FTH")),
      MockPatient("4", defaultFullPatient(familyId = Some("2"), position = "PROBAND")),
      MockPatient("5", defaultFullPatient(familyId = Some("2"), position = "MTH"))
    )
    val families: Map[String, Seq[FamilyExtension]] = FamilyExtension.buildFamilies(patients)
    families.size shouldBe 2
    families.get("1") shouldBe Some(Seq(
      FamilyExtension(new IdType("2"), "MTH"),
      FamilyExtension(new IdType("3"), "FTH")
    ))
    families.get("2") shouldBe Some(Seq(
      FamilyExtension(new IdType("5"), "MTH")
    ))
  }
}
