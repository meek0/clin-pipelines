package bio.ferlab.clin.etl.testutils

import bio.ferlab.clin.etl.testutils.containers.ElasticSearchContainer

trait WholeStack extends MinioServer with FhirServer with ElasticSearchServer {

}

trait WholeStackSuite extends MinioServer with FhirServerSuite with ElasticSearchServerSuite {

}

object StartWholeStack extends App with MinioServer with FhirServer with ElasticSearchServer {
  LOGGER.info("Whole stack is started")
  while (true) {

  }
}




