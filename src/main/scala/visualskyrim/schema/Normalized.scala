package visualskyrim.schema

import org.apache.spark.sql.Row
import org.joda.time.DateTime
import visualskyrim.common.DateTimeUtils

import scala.util.{Failure, Success, Try}

case class Normalized(timestamp: Int, clientIP: String, useragent: String, clientId: String)

object Normalized {

  def apply(row: Row): Option[Normalized] = Try {
    val timestamp = (DateTimeUtils.fromISO8601Hour(row.getString(0)).getMillis / 1000).toInt
    val clientIP = row.getString(2)
    val userAgent = row.getString(12)

    Normalized(timestamp, clientIP, userAgent, toClientId(clientIP))
  } match {
    case Success(norm: Normalized) => Some(norm)
    case Failure(e) => None
  }

  private def toClientId(clientIP: String) = clientIP
}
