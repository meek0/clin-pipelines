package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.testutils.WithFhirServer
import org.hl7.fhir.r4.model.{Attachment, DocumentReference, UnsignedIntType}
import org.hl7.fhir.r4.model.DocumentReference.DocumentReferenceContentComponent
import org.hl7.fhir.r4.model.Enumerations.DocumentReferenceStatus

import scala.collection.JavaConverters._
import java.util.Base64

object TestMd5 extends App with WithFhirServer{

  val md5 = "d1b484d132ddfd1774481044bbea07ce"

  val b = md5.getBytes()

  val encodedString: String =
    Base64.getEncoder().withoutPadding().encodeToString(b);
  val encodedString2: String =
    Base64.getEncoder().encodeToString(b);
  println(encodedString)
  println(encodedString2)


  val dr = new DocumentReference()
  dr.setStatus(DocumentReferenceStatus.CURRENT)

  val a = new Attachment()
  a.setContentType("application/binary")
  a.setUrl(s"https://objectstore.cqgc.ca/1234")
  //https://www.baeldung.com/java-base64-encode-and-decode#2-java-8-base64-encoding-without-padding
  val str = Base64.getEncoder.withoutPadding().encodeToString(md5.getBytes())
  val hash = Base64.getDecoder().decode(str)
  a.setHash(hash)
  a.setTitle("test")
  a.setSizeElement(new UnsignedIntType(10))
  val drcc = new DocumentReferenceContentComponent(a)
  dr.setContent(List(drcc).asJava)
  println(fhirServer.fhirContext.newJsonParser.setPrettyPrint(true).encodeResourceToString(dr))
  println(new String(Base64.getDecoder.decode("ZDFiNDg0ZDEzMmRkZmQxNzc0NDgxMDQ0YmJlYTA3Y2U=")))

//  fhirClient.create().resource(dr).execute()
//  println(b.map(_.toChar).mkString )


}
