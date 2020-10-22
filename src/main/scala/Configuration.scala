import scala.util.Properties

object Configuration {
  // Object Store - S3
  val accessKey:String = getConfiguration("ACCESS_KEY", "minio")
  val secretKey:String = getConfiguration("SECRET_KEY", "minio123")
  val serviceEndpoint:String = getConfiguration("SERVICE_ENDPOINT", "http://127.0.0.1:9000")
  val bucket:String = getConfiguration("BUCKET", "clin")
  val signinRegion:String = getConfiguration("SIGN_IN_REGION", "chusj")

  // HAPI FHIR
  val fhirServerBase:String = getConfiguration("FHIR_SERVER_BASE", "https://fhir.qa.clin.ferlab.bio/fhir")

  // KeyCloak
  val keycloakAuthUrl:String = getConfiguration("KEYCLOAK_AUTH_URL", "https://auth.qa.cqdg.ferlab.bio/auth/realms/clin/protocol/openid-connect/token")
  val keycloakAuthClientId:String = getConfiguration("KEYCLOAK_AUTH_CLIENT_ID", "clin-system")
  val keycloakAuthClientSecret:String = getConfiguration("KEYCLOAK_AUTH_CLIENT_SECRET", "__UNDEFINED__")


  def getConfiguration(key: String, default: String): String ={
    Properties.envOrElse(key, Properties.propOrElse(key, default))
  }

  /*
   * FOR LOCAL DEBUGGING ONLY - Do not print secrets in logs!
   *
  override def toString() : String = {
    return "{\n" +
      s"\taccessKey : $accessKey,\n" +
      s"\tsecretKey : $secretKey,\n" +
      s"\tserviceEndpoint: $serviceEndpoint,\n" +
      s"\tsigninRegion: $signinRegion\n" +
    "}"
  }
   */
}
