package bio.ferlab.clin.etl.task.ldmnotifier.model

import play.api.libs.json.{Json, Reads}

case class Data(taskList: Option[Seq[Task]])

object Data {
  implicit val reads: Reads[Data] = Json.reads[Data]
}

case class GqlResponse(data: Option[Data])

object GqlResponse {
  implicit val reads: Reads[GqlResponse] = Json.reads[GqlResponse]
}
