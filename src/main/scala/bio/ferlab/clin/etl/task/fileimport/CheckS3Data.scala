package bio.ferlab.clin.etl.task.fileimport

import bio.ferlab.clin.etl.conf.AWSConf
import bio.ferlab.clin.etl.isValid
import bio.ferlab.clin.etl.s3.S3Utils
import bio.ferlab.clin.etl.s3.S3Utils.getContent
import bio.ferlab.clin.etl.task.fileimport.model.{FileEntry, Metadata, RawFileEntry}
import cats.data.ValidatedNel
import org.apache.commons.lang3.StringUtils
import org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CopyRequest}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{CompletableFuture, Executors}
import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CheckS3Data {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def ls(bucket: String, prefix: String, maxKeys: Int = 4500)(implicit s3Client: S3Client): List[RawFileEntry] = {
    val lsRequest = ListObjectsV2Request.builder().bucket(bucket).maxKeys(maxKeys).prefix(prefix).build()

    nextBatch(s3Client, s3Client.listObjectsV2(lsRequest), maxKeys)
  }

  @tailrec
  private def nextBatch(s3Client: S3Client, listing: ListObjectsV2Response, maxKeys: Int, objects: List[RawFileEntry] = Nil): List[RawFileEntry] = {
    val pageKeys = listing.contents().asScala.map(o => RawFileEntry(listing.name(), o.key(), o.size())).toList

    if (listing.isTruncated) {
      val nextRequest = ListObjectsV2Request.builder().bucket(listing.name).prefix(listing.prefix()).continuationToken(listing.nextContinuationToken()).build()
      nextBatch(s3Client, s3Client.listObjectsV2(nextRequest), maxKeys, pageKeys ::: objects)
    } else
      pageKeys ::: objects
  }

  def validateFileEntries(rawFileEntries: Seq[RawFileEntry], fileEntries: Seq[FileEntry]): ValidatedNel[String, Seq[FileEntry]] = {
    LOGGER.info("################# Validate File entries ##################")
    val fileEntriesNotInAnalysis = rawFileEntries.filterNot(r => r.isChecksum || fileEntries.exists(f => f.key == r.key))
    val errorFilesNotExist = fileEntriesNotInAnalysis.map(f => s"File ${f.filename} not found in metadata JSON file.")
    isValid(fileEntries, errorFilesNotExist)
  }

  def loadRawFileEntries(bucket: String, prefix: String)(implicit s3Client: S3Client): Seq[RawFileEntry] = {
    val fileEntries = ls(bucket, prefix)
      .filter(f => !f.key.contains("logs")
        && f.filename != ""
        && f.filename != "_SUCCESS"
        && f.filename != "metadata.json"
        && !f.filename.toLowerCase().contains("hard-filtered.formatted.norm.vep.vcf.gz")
        && !f.filename.toLowerCase().contains("dragen.wes_somatic-tumor_only.hard-filtered.norm.vep.vcf.gz")
        && !f.filename.toLowerCase().contains("hard-filtered.vcf.gz")
        && !f.filename.toLowerCase().endsWith("extra_results.tgz")
        && !f.filename.toLowerCase().endsWith(".hpo"))
    fileEntries
  }

  def idForPrefix(outputPrefix:String, id: => String): String = {
    if(StringUtils.isEmpty(outputPrefix)) id else s"$outputPrefix/$id"
  }

  def loadFileEntries(m: Metadata, fileEntries: Seq[RawFileEntry], outputPrefix: String, generateId: () => String = () => UUID.randomUUID().toString)(implicit s3Client: S3Client): Seq[FileEntry] = {
    val (checksums, files) = fileEntries.partition(_.isChecksum)
    val mapOfIds = m.analyses.flatMap { a =>
      val cramId: String = idForPrefix(outputPrefix,generateId())
      val craiId: String = s"$cramId.crai"
      val snvVcfId: String = idForPrefix(outputPrefix,generateId())
      val snvTbiId: String = s"$snvVcfId.tbi"
      val cnvVcfId: String = idForPrefix(outputPrefix,generateId())
      val cnvTbiId: String = s"$cnvVcfId.tbi"
      val svVcfId: String = idForPrefix(outputPrefix,generateId())
      val svTbiId: String = s"$svVcfId.tbi"
      val qcId: String = idForPrefix(outputPrefix,generateId())
      ///exomiser
      val exomiserId: String = idForPrefix(outputPrefix,generateId())
      val exomiserHtmlId: String =s"$exomiserId.html"
      val exomiserJsonId: String = s"$exomiserId.json"
      val exomiserVariantsTsvId: String = s"$exomiserId.variants.tsv"
      // igv
      val igvTrackId: String = idForPrefix(outputPrefix,generateId())
      val segBw: String = s"$igvTrackId.seg.bw"
      val hardFilteredBaf: String = s"$igvTrackId.baf.bw"
      val rohBed: String = s"$igvTrackId.roh.bed"
      val hyperExomeHg38Bed: String = s"$igvTrackId.exome.bed"

      val cnvCallsPng: String = idForPrefix(outputPrefix,generateId())
      val coverageByGeneCsv: String = idForPrefix(outputPrefix,generateId())
      val qcMetrics: String = idForPrefix(outputPrefix,generateId())
      val qcMetricsTsv: String = idForPrefix(outputPrefix,generateId())

      Seq(
        a.files.cram -> (cramId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.cram)),
        a.files.crai -> (craiId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.crai)),
        a.files.snv_vcf -> (snvVcfId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.snv_vcf)),
        a.files.snv_tbi -> (snvTbiId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.snv_tbi)),
        a.files.cnv_vcf -> (cnvVcfId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.cnv_vcf)),
        a.files.cnv_tbi -> (cnvTbiId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.cnv_tbi)),
        a.files.sv_vcf.orNull -> (svVcfId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.sv_vcf.orNull)),
        a.files.sv_tbi.orNull -> (svTbiId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.sv_tbi.orNull)),
        a.files.supplement -> (qcId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.supplement)),
        a.files.exomiser_html.orNull -> (exomiserHtmlId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.exomiser_html.orNull)),
        a.files.exomiser_json.orNull -> (exomiserJsonId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.exomiser_json.orNull)),
        a.files.exomiser_variants_tsv.orNull -> (exomiserVariantsTsvId, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.exomiser_variants_tsv.orNull)),
        a.files.seg_bw.orNull -> (segBw, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.seg_bw.orNull)),
        a.files.hard_filtered_baf_bw.orNull -> (hardFilteredBaf, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.hard_filtered_baf_bw.orNull)),
        a.files.roh_bed.orNull -> (rohBed, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.roh_bed.orNull)),
        a.files.hyper_exome_hg38_bed.orNull -> (hyperExomeHg38Bed, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.hyper_exome_hg38_bed.orNull)),
        a.files.cnv_calls_png.orNull -> (cnvCallsPng, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.cnv_calls_png.orNull)),
        a.files.coverage_by_gene_csv.orNull -> (coverageByGeneCsv, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.coverage_by_gene_csv.orNull)),
        a.files.qc_metrics.orNull -> (qcMetrics, APPLICATION_OCTET_STREAM.getMimeType, attach(a.files.qc_metrics.orNull)),
      )

    }.toMap
    files
      .flatMap { f =>
        mapOfIds.get(f.filename).map {
          case (id, contentType, contentDisposition) =>
            val md5sum = checksums.find(c => c.filename.contains(f.filename))
              .map { c => getContent(c.bucket, c.key).strip() }
            FileEntry(f, id, md5sum, contentType, contentDisposition)
        }
      }

  }

  def attach(f: String) = {
    s"""attachment; filename="$f""""
  }

  def revert(files: Seq[FileEntry], bucketDest: String)(implicit s3Client: S3Client): Unit = {
    LOGGER.info("################# !!!! ERROR : Reverting Copy Files !!! ##################")
    files.foreach { f =>
      val del = DeleteObjectRequest.builder().bucket(bucketDest).key(f.id).build()
      s3Client.deleteObject(del)
    }
  }

  private def buildCopyObjectRequest(f: FileEntry, bucketDest: String) = {
    CopyObjectRequest.builder()
      .sourceBucket(f.bucket)
      .sourceKey(f.key)
      .contentType(f.contentType)
      .contentDisposition(f.contentDisposition)
      .destinationBucket(bucketDest)
      .destinationKey(f.id)
      .metadataDirective(MetadataDirective.REPLACE)
      .build()
  }

  def copyFiles(files: Seq[FileEntry], bucketDest: String)(implicit s3Client: S3Client): Unit = {
    LOGGER.info("################# Copy Files ##################")
    files.foreach { f =>
      val cp = buildCopyObjectRequest(f, bucketDest)
      s3Client.copyObject(cp)
    }
  }

  def copyFilesAsync(files: Seq[FileEntry], bucketDest: String)(implicit AWSConf: AWSConf): Unit = {
    LOGGER.info("################# Copy Files ##################")
    val s3Client = S3Utils.buildAsyncS3Client(AWSConf)
    val transferManager = S3TransferManager.builder()
      .s3Client(s3Client)
      .executor(Executors.newCachedThreadPool()).build()
    val copies = files.map { f =>
      val cp = buildCopyObjectRequest(f, bucketDest)
      val copyRequest = CopyRequest.builder()
        .copyObjectRequest(cp)
        .build()
      transferManager.copy(copyRequest).completionFuture()
    }.toList
    CompletableFuture.allOf(copies: _*).join()
    transferManager.close()
  }
}