package bio.ferlab.clin.etl.db

import bio.ferlab.clin.etl.conf.UsersDbConf
import java.sql.{Connection, DriverManager, ResultSet}

class UsersDbClient(conf: UsersDbConf) {

  private lazy val conn: Connection = {
    Class.forName("org.postgresql.Driver")
    var connectionUrl: String = s"jdbc:postgresql://${conf.host}:${conf.port}/${conf.db}?ssl=${conf.ssl}"
    if (conf.ssl == "true") {
      connectionUrl += s"&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory&sslmode=${conf.sslMode}&sslrootcert=${conf.sslRootCert}"
    } else {
      connectionUrl += "&sslfactory=org.postgresql.ssl.NonValidatingFactory"
    }
    DriverManager.getConnection(connectionUrl, conf.user, conf.password)
  }

  def findVariants(): Seq[Variant] = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM variants")
    var results = Seq[Variant]()
    while (rs.next()) {
      results = results :+ Variant(
        rs.getString("id"),
        rs.getString("unique_id"),
        rs.getString("author_id"),
        rs.getString("organization_id"),
        rs.getTimestamp("timestamp"),
        rs.getString("properties")
      )
    }
    rs.close()
    stmt.close()
    results
  }

  def updateVariant(id: String, uniqueId: String): Int = {
    val stmt = conn.createStatement()
    val count = stmt.executeUpdate(s"UPDATE variants SET unique_id = '$uniqueId' WHERE id = '$id'")
    stmt.close()
    count
  }

}
