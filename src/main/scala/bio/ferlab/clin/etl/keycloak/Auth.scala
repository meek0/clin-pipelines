package bio.ferlab.clin.etl.keycloak

import org.keycloak.authorization.client.{AuthzClient, Configuration}
import org.keycloak.common.util.Time
import org.keycloak.representations.idm.authorization.AuthorizationRequest
import scala.collection.JavaConverters._

object Auth {

  private val config = new Configuration()
  config.setRealm(sys.env("KEYCLOAK_REALM")) //clin
  config.setAuthServerUrl(sys.env("KEYCLOAK_URL")) //https://auth.qa.cqdg.ferlab.bio/auth)
  config.setResource(sys.env("KEYCLOAK_CLIENT_KEY")) //clin-system
  config.setCredentials(Map("secret" -> sys.env("KEYCLOAK_CLIENT_SECRET")).toMap[String, Object].asJava)
  private val authzClient = AuthzClient.create(config)

  private val req = new AuthorizationRequest()
  req.setAudience(sys.env("KEYCLOAK_AUTHORIZATION_AUDIENCE")) //clin-acl

  private var expiresAt = 0L
  private var token = ""

  def withToken[T](f: String => T): T = {
    if (expiresAt == 0 || expiresAt > Time.currentTime()) {
      val resp = authzClient.authorization().authorize(req)
      expiresAt = Time.currentTime + resp.getExpiresIn - 5
      token = resp.getToken
    }
    f(token)
  }


}
