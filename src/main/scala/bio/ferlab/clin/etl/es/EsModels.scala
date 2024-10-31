package bio.ferlab.clin.etl.es

case class EsHPO(hpo_id: String, name: String)
case class EsCNV(name: String, aliquotId: String, alternate: String, serviceRequestId: String, hash: String)