package bio.ferlab.clin.etl.scripts

import bio.ferlab.clin.etl.ValidationResult
import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.db.{UsersDbClient, Variant}
import bio.ferlab.clin.etl.es.{EsCNV, EsClient}
import ca.uhn.fhir.rest.client.api.IGenericClient
import cats.data.Validated.{Invalid, Valid}
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import java.math.BigInteger
import java.security.MessageDigest

object FixFlagHashes {

  val LOGGER: Logger = LoggerFactory.getLogger(FixFerloadURLs.getClass)

  def apply(conf: Conf, params: Array[String]): ValidationResult[Boolean] = {

    val cnvIndex = params(0)
    val dryRun = params.contains("--dryrun")

    if (StringUtils.isBlank(cnvIndex) || !cnvIndex.contains("cnv")) {
      throw new IllegalArgumentException("Usage: FixFlagsHash <cnv_index> [--dryrun]")
    }

    val esClient = new EsClient(conf.es)
    val dbClient = new UsersDbClient(conf.usersDb)

    val variants = dbClient.findVariants()   // no pagination but we wont have much in theory
    if (variants.isEmpty) {
      throw new IllegalStateException("No variants found in users-db")
    }
    if (!variants.exists(v => v.uniqueId.endsWith("_cnv"))) {
      throw new IllegalStateException("No CNVs found in users-db")
    }
    LOGGER.info(s"Found ${variants.size} variants in users-db")
    variants.foreach(v => LOGGER.info(s"=> $v"))

    var lastId = ""
    var cnvCount = 0
    val size = 10000
    do {
      LOGGER.info(s"Fetch CNVs from ES from: ${cnvCount} size: $size")
      val (currentLastId, cnvs) = esClient.getCNVsWithPagination(cnvIndex, lastId, size)
      cnvs.foreach(cnv => {

        val newFashionHash = sha1(s"${cnv.name}-${cnv.alternate}-${cnv.serviceRequestId}")
        if (newFashionHash.equals(cnv.hash)) {
          // all possible previous hashes for this CNV
          val oldFashionHash01 = sha1(s"${cnv.name}-${cnv.aliquotId}-${cnv.alternate}")
          val oldFashionHash02 = sha1(s"${cnv.name}-${cnv.serviceRequestId}")
          val oldUniqueId01 = formatCNVUniqueId(oldFashionHash01, cnv)
          val oldUniqueId02 = formatCNVUniqueId(oldFashionHash02, cnv)

          val newUniqueId = formatCNVUniqueId(newFashionHash, cnv)
          variants.filter(v => List(oldUniqueId01, oldUniqueId02).contains(v.uniqueId)).foreach(variant => {
            LOGGER.info(s"Updating CNV: $cnv Variant: $variant")
            if (!dryRun) {
              if (dbClient.updateVariant(variant.id, newUniqueId) != 1) {
                throw new IllegalStateException(s"Failed to update variant ${variant.id}")
              }
            }
          })
        }
      })
      lastId = currentLastId
      cnvCount += cnvs.size
    } while(lastId != "")
    Valid(true)
  }

  def formatCNVUniqueId(hash: String, cnv: EsCNV): String = {
    s"${hash}_${cnv.analysisServiceRequestId}_${cnv.patientId}_cnv"
  }

  val md: MessageDigest = MessageDigest.getInstance("SHA-1")
  def sha1(input: String): String = {
    val digest: Array[Byte] = md.digest(input.getBytes("UTF-8"))
    val bigInt = new BigInteger(1, digest)
    val output: String = bigInt.toString(16)

    // Pad with leading zeros
    "0" * (40 - output.length) + output
  }

}
