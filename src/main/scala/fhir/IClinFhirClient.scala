package fhir

import ca.uhn.fhir.rest.annotation.{IdParam, Read}
import ca.uhn.fhir.rest.client.api.IBasicClient
import org.hl7.fhir.r4.model._

//https://github.com/jamesagnew/hapi-fhir/blob/master/hapi-fhir-structures-r4/src/test/java/ca/uhn/fhir/rest/client/ITestClient.java
trait IClinFhirClient extends IBasicClient {
  @Read(`type` = classOf[Patient])
  def getPatientById(@IdParam id: IdType): Patient

  @Read(`type` = classOf[Organization])
  def getOrganizationById(@IdParam id: IdType): Organization

  @Read(`type` = classOf[Practitioner])
  def getPractitionerById(@IdParam id: IdType): Practitioner

  @Read(`type` = classOf[ServiceRequest])
  def getServiceRequestById(@IdParam id: IdType): ServiceRequest
}
