package bio.ferlab.clin.etl.db

import java.sql.Timestamp

case class Variant(id: String, uniqueId: String, authorId: String, organizationId: String, timestamp: Timestamp, properties: String)