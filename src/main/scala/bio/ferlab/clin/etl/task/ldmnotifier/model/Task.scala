package bio.ferlab.clin.etl.task.ldmnotifier.model
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._
import play.api.libs.json.{Json, Reads}

case class Attachment(url: String, hash64: Option[String], title: String, size: Long)
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

case class Requester(id: String, alias: String, email: Seq[String])
object Requester{
  implicit val reads: Reads[Requester] = Json.reads[Requester]
}

case class Task(id: String, requester: Requester, documents: Seq[Document], serviceRequestReference: String)
object Task {
  // FHIR can return either a single document or a list of documents
  // explain the JSON parser how to manage this case
  implicit val eitherReads: Reads[Either[Seq[Document], Document]] =
    Reads[Either[Seq[Document], Document]] { json =>
      json.validate[Seq[Document]].map(Left(_)).orElse(json.validate[Document].map(Right(_)))
    }

  // unfortunately, we also have to describe how to parse the Task object
  implicit val reads: Reads[Task] =
    ((__ \ "id").read[String] and
      (__ \ "requester").read[Requester] and
      (__ \ "documents").read[Either[Seq[Document], Document]].map(e => e.fold(identity, Vector(_))) and
      (__ \ "serviceRequestReference").read[String]
      ) (Task(_, _, _, _))
}