package visualskyrim.schema

import org.apache.spark.sql.Row
import org.joda.time.DateTime
import visualskyrim.common.DateTimeUtils

import scala.util.{Failure, Success, Try}

case class Normalized(timestamp: Int, clientIP: String, useragent: String, url: String, clientId: String)

object Normalized {

  def apply(row: Row): Either[Normalized, String] = Try {
    val timestamp = (DateTimeUtils.fromISO8601Hour(row.getString(0)).getMillis / 1000).toInt
    val clientIP = row.getString(2)
    val url = row.getString(11).split(" ") match {
      case Array(_: String, url: String, _: String) => url
      case _ => throw new Exception("Fail to parse url.")
    }
    val userAgent = row.getString(12)

    Normalized(timestamp, clientIP, userAgent, url, toClientId(clientIP))
  } match {
    case Success(norm: Normalized) => Left(norm)
    case Failure(e) => Right(row.toString())
  }

  private def toClientId(clientIP: String) = clientIP
}
