import scala.util.Properties

object Configuration {
  val accessKey = getConfiguration("ACCESS_KEY", "minio")
  val secretKey = getConfiguration("SECRET_KEY", "minio123")
  val serviceEndpoint = getConfiguration("SERVICE_ENDPOINT", "http://127.0.0.1:9000")
  val bucket = getConfiguration("BUCKET", "clin")
  val signinRegion = getConfiguration("SIGN_IN_REGION", "chusj")

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
