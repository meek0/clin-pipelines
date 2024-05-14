package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.conf.{Conf, FerloadConf, FhirConf, KeycloakConf}
import bio.ferlab.clin.etl.fhir.FhirUtils
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.CodingSystems
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Extensions.SEQUENCING_EXPERIMENT
import bio.ferlab.clin.etl.fhir.FhirUtils.Constants.Profiles.{ANALYSIS_SERVICE_REQUEST, SEQUENCING_SERVICE_REQUEST}
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.task.fileimport.model.TTask.{EXOME_GERMLINE_ANALYSIS, EXTUM_ANALYSIS, SOMATIC_NORMAL}
import bio.ferlab.clin.etl.task.fileimport.model.{TBundle, TTask}
import bio.ferlab.clin.etl.testutils.{FhirTestUtils, WholeStackSuite}
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender
import org.hl7.fhir.r4.model._
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import java.util.Date
import scala.collection.JavaConverters._
import scala.io.Source

class SomaticNormalImportSpec extends FlatSpec with WholeStackSuite with Matchers {

  val keycloakConf = KeycloakConf(null, null, null, null, null)
  val fhirConf = FhirConf(fhirBaseUrl)
  val conf = new Conf(awsConf, keycloakConf, fhirConf, ferloadConf, null, null)

  "list S3 VCFs" should "fail no VCF files found" in {
    withS3Objects { (inputPrefix, _) =>
      try {
        SomaticNormalImport(inputPrefix, Array())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.equals(s"No VCF files found in: $inputPrefix"))
      }
    }
  }

  "list S3 VCFs" should "fail no attached tbi found" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/no_attached_tbi", inputBucket)
      try {
        SomaticNormalImport(inputPrefix, Array())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.equals(s"Cant find TBI file for: $inputPrefix/empty_0.somatic_tumor_normal.vcf.gz\n" +
            s"Cant find TBI file for: $inputPrefix/empty_1.somatic_tumor_normal.vcf.gz"))
      }
    }
  }

  "extractAliquotIDs" should "fail no aliquot IDs found" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/no_aliquot_ids", inputBucket)
      try {
        SomaticNormalImport(inputPrefix, Array())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.equals(s"$inputPrefix/invalid.somatic_tumor_normal.vcf.gz no aliquot IDs found"))
      }
    }
  }

  "extractAliquotIDs" should "fail more than two aliquot IDs found" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/more_than_two_aliquot_ids", inputBucket)
      try {
        SomaticNormalImport(inputPrefix, Array())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.equals(s"$inputPrefix/invalid.somatic_tumor_normal.vcf.gz contains more than 2 aliquot IDs"))
      }
    }
  }

  "findFhirTasks" should "fail missing GEBA or TEBA task" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/good", inputBucket)
      try {
        SomaticNormalImport(inputPrefix, Array())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.equals("Can't find all required FHIR Tasks for aliquot IDs: 00001 00002"))
      }
    }
  }

  "findFhirTasks" should "fail VCF name doesn't match aliquot IDs" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/vcf_name_doesnt_match", inputBucket)

      // prepare FHIR
      val (specimenGermline, specimenSomatic, taskGermline, taskSomatic, _, _) = prepareFhir()
      TBundle(FhirUtils.bundleCreate(Seq(specimenGermline, specimenSomatic, taskGermline, taskSomatic)).toList).save()

      try {
        SomaticNormalImport(inputPrefix, Array())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.equals("0000X.0000Y.somatic_tumor_normal.vcf.gz file name doesn't match aliquot IDs inside"))
      }
    }
  }

  "findFhirTasks" should "fail tasks don't reference the same patient" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/good", inputBucket)

      // prepare FHIR
      val patient1 = new Patient()
      patient1.setId("p1")

      val patient2 = new Patient()
      patient2.setId("p2")

      val (specimenGermline, specimenSomatic, taskGermline, taskSomatic, _, _) = prepareFhir()
      taskGermline.setFor(new Reference("Patient/p1"))
      taskSomatic.setFor(new Reference("Patient/p2"))

      TBundle(FhirUtils.bundleCreate(Seq(specimenGermline, specimenSomatic, patient1, patient2,taskGermline, taskSomatic)).toList).save()

      try {
        SomaticNormalImport(inputPrefix, Array())(conf)
        fail("Expecting IllegalStateException")
      } catch {
        case e: IllegalStateException =>
          println(e.getMessage)
          assert(e.getMessage.startsWith("Tasks don't have the same patient for reference"))
      }
    }
  }

  "findFhirTasks" should "do nothing TNEBA already exists" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/good", inputBucket)

      // prepare FHIR
      val (specimenGermline, specimenSomatic, taskGermline, taskSomatic, patient, taskTNEBA) = prepareFhir(withPatient = true, withTNEBA = true)
      TBundle(FhirUtils.bundleCreate(Seq(specimenGermline, specimenSomatic, patient, taskGermline, taskSomatic, taskTNEBA)).toList).save()

      SomaticNormalImport(inputPrefix, Array())(conf)
    }
  }

  "findFhirTasks" should "create TNEBA task and copy S3 files" in {
    withS3Objects { (inputPrefix, _) =>
      // prepare S3
      transferFromResources(inputPrefix, "somatic_normal/good", inputBucket)

      // prepare FHIR
      val (specimenGermline, specimenSomatic, taskGermline, taskSomatic, patient, _) = prepareFhir(withPatient = true)
      TBundle(FhirUtils.bundleCreate(Seq(specimenGermline, specimenSomatic, patient, taskGermline, taskSomatic)).toList).save()

      SomaticNormalImport(inputPrefix, Array())(conf)

      val copiedS3Files = list(outputBucket, "")
      assert(copiedS3Files.size == 2)
      assert(copiedS3Files(0) + ".tbi" == copiedS3Files(1))

      val resultBundle = fhirClient.search().forResource(classOf[Task]).returnBundle(classOf[Bundle]).execute()
      val allTasks = resultBundle.getEntry.asScala.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Task] }
      assert(allTasks.size == 3)

      val taskTNEBA = allTasks.find(t => t.getCode.getCodingFirstRep.getCode == "TNEBA").get
      assert(taskTNEBA.getOutput.size() == 1)
      assert(taskTNEBA.getAuthoredOn != null)

      // TODO more validation on TNEBA task
    }
  }

  private def prepareFhir(withPatient: Boolean = false, withTNEBA: Boolean = false): (Specimen, Specimen, Task, Task, Patient, Task) = {

    FhirTestUtils.clearAll()

    val specimenGermline = new Specimen()
    specimenGermline.setId("germline")

    val specimenSomatic = new Specimen()
    specimenSomatic.setId("somatic")

    val taskGermline = new Task()
    taskGermline.getCode.getCodingFirstRep.setCode(EXOME_GERMLINE_ANALYSIS)
    taskGermline.addExtension().setUrl(SEQUENCING_EXPERIMENT).addExtension().setUrl("labAliquotId").setValue(new StringType("00002"))
    taskGermline.getInputFirstRep.setValue(new Reference("Specimen/germline").setDisplay("Submitter Sample ID: germline_id"))

    val taskSomatic = new Task()
    taskSomatic.getCode.getCodingFirstRep.setCode(EXTUM_ANALYSIS)
    taskSomatic.addExtension().setUrl(SEQUENCING_EXPERIMENT).addExtension().setUrl("labAliquotId").setValue(new StringType("00001"))
    taskSomatic.getInputFirstRep.setValue(new Reference("Specimen/somatic").setDisplay("Submitter Sample ID: somatic_id"))
    taskSomatic.setAuthoredOn(new Date())

    var patient:Patient = null
    if (withPatient){
      patient = new Patient()
      patient.setId(IdType.newRandomUuid())

      taskGermline.setFor(new Reference(patient.getIdElement.getIdPart))
      taskSomatic.setFor(taskGermline.getFor)
    }

    var taskTNEBA: Task = null
    if (withTNEBA) {
      taskTNEBA = new Task()
      taskTNEBA.getCode.getCodingFirstRep.setCode(SOMATIC_NORMAL)
      taskTNEBA.addExtension().setUrl(SEQUENCING_EXPERIMENT).addExtension().setUrl("labAliquotId").setValue(new StringType("00001"))
    }

    (specimenGermline, specimenSomatic, taskGermline, taskSomatic, patient, taskTNEBA)
  }

}