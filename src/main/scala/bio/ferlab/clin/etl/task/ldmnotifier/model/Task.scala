package bio.ferlab.clin.etl.task.ldmnotifier.model

import play.api.libs.json.{Json, Reads}

case class Attachment(url: String, hash64: Option[String], title: String)
object Attachment {
  implicit val reads: Reads[Attachment] = Json.reads[Attachment]
}

case class Content(attachment: Attachment, fileFormat: String)
object Content {
  implicit val reads: Reads[Content] = Json.reads[Content]
}

case class Sample(sampleId: String)
object Sample {
  implicit val reads: Reads[Sample] = Json.reads[Sample]
}

case class Document(contentList: Seq[Content], sample: Sample, patientReference: String, fileType: String)
object Document {
  implicit val reads: Reads[Document] = Json.reads[Document]
}

case class Requester(id: String, alias: String, email: String)
object Requester{
  implicit val reads: Reads[Requester] = Json.reads[Requester]
}

case class Task(id: String, requester: Requester, documents: Seq[Document], serviceRequestReference: String)
object Task {
  implicit val reads: Reads[Task] = Json.reads[Task]
}


