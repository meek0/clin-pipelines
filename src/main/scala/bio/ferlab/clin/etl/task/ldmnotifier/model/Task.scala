package bio.ferlab.clin.etl.task.ldmnotifier.model

import play.api.libs.json.{Json, Reads}

case class Url(url: String)
object Url {
  implicit val reads: Reads[Url] = Json.reads[Url]
}
case class Attachments(urls: Seq[Url])
object Attachments {
  implicit val reads: Reads[Attachments] = Json.reads[Attachments]
}

case class Owner(id: String, alias: String, email: String)
object Owner{
  implicit val reads: Reads[Owner] = Json.reads[Owner]
}

case class Task(id: String, owner: Owner, attachments: Attachments)
object Task {
  implicit val reads: Reads[Task] = Json.reads[Task]
}


