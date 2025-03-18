package bio.ferlab.clin.etl.testutils

import bio.ferlab.clin.etl.testutils.containers.ElasticSearchContainer

trait WholeStack extends MinioServer with ElasticSearchServer with FhirServer {

}

trait WholeStackSuite extends MinioServer with ElasticSearchServerSuite with FhirServerSuite {

}

object StartWholeStack extends App with MinioServer with ElasticSearchServer with FhirServer {
  LOGGER.info("Whole stack is started")
  while (true) {

  }
}




