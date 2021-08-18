package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.fhir.FhirClient.buildFhirClients
import bio.ferlab.clin.etl.fhir.FhirUtils.{ResourceExtension, bundleDelete, bundleEntryUpdate}
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.s3.S3Utils.buildS3Client
import bio.ferlab.clin.etl.task.fileimport.model.TBundle
import ca.uhn.fhir.model.api.Include
import ca.uhn.fhir.rest.client.api.IGenericClient
import ca.uhn.fhir.rest.gclient.StringClientParam
import cats.data.Validated.Valid
import org.hl7.fhir.r4.model.Bundle.SearchEntryMode
import org.hl7.fhir.r4.model.Specimen.INCLUDE_PARENT
import org.hl7.fhir.r4.model._
import software.amazon.awssdk.services.s3.S3Client

import scala.collection.JavaConverters._


object DeleteBatch extends App {
  withSystemExit {
    withLog {
      withConf { conf =>
        val (bucket, prefix, runName, deleteSpecimens) = args match {
          case Array(b, p, r) => (b, p, r, false)
          case Array(b, p, r, "true") => (b, p, r, true)
          case Array(b, p, r, "false") => (b, p, r, false)
        }
        implicit val s3Client: S3Client = buildS3Client(conf.aws)
        val (_, client) = buildFhirClients(conf.fhir, conf.keycloak)
        withReport(bucket, prefix) { reportPath =>
          implicit val c: IGenericClient = client
          val baseQuery = client.search().forResource(classOf[Task])
            .where(new StringClientParam("run-name")
              .matchesExactly()
              .value(runName)) //"201106_A00516_0169_AHFM3HDSXY"
            .include(new Include("Task:output_documentreference").asRecursive())
            .include(new Include("Task:input_specimen"))
          val query = if (deleteSpecimens) {
            baseQuery.include(INCLUDE_PARENT.asRecursive())
              .include(Task.INCLUDE_FOCUS)
          } else baseQuery
          val results = query
            .count(2000)
            .encodedJson()
            .returnBundle(classOf[Bundle]).execute()

          val entries: Seq[Bundle.BundleEntryComponent] = results.getEntry.asScala.toSeq

          val tasks = entries.collect { case be if be.getSearch.getMode == SearchEntryMode.MATCH => be.getResource.asInstanceOf[Task] }
          val specimens = entries.collect { case be if be.getSearch.getMode == SearchEntryMode.INCLUDE && be.getResource.isInstanceOf[Specimen] => be.getResource.asInstanceOf[Specimen] }
          val serviceRequests = entries.collect { case be if be.getSearch.getMode == SearchEntryMode.INCLUDE && be.getResource.isInstanceOf[ServiceRequest] => be.getResource.asInstanceOf[ServiceRequest] }
          val documentReferences = entries.collect { case be if be.getSearch.getMode == SearchEntryMode.INCLUDE && be.getResource.isInstanceOf[DocumentReference] => be.getResource.asInstanceOf[DocumentReference] }

          val bundle = if (deleteSpecimens) {
            val specimenReferences = specimens.map(s => s.toReference().getReference)
            val toUpdate = serviceRequests.map { sr =>
              sr.setSpecimen(sr.getSpecimen.asScala.filterNot(s => specimenReferences.contains(s.getReference)).asJava)
              bundleEntryUpdate(sr)
            }
            val toDelete = bundleDelete(tasks ++ documentReferences ++ specimens)
            TBundle((toUpdate ++ toDelete).toList)
          } else {
            TBundle(bundleDelete(tasks ++ documentReferences).toList)
          }

          writeAheadLog(bucket, reportPath, bundle, "delete_batch_bundle")
          bundle.save()
          Valid("")
        }
      }
    }
  }


  def writeAheadLog(inputBucket: String, reportPath: String, bundle: TBundle, bundleName: String)(implicit s3: S3Client, client: IGenericClient): Unit = {
    S3Utils.writeContent(inputBucket, s"$reportPath/$bundleName.json", bundle.print())
  }
}
