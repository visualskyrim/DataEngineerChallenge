package visualskyrim.common

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

object DateTimeUtils {

  private val batchHourFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH")
  private val iso8601Formatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

  def fromBatchHour(batchHour: String): DateTime = batchHourFormatter.withZone(DateTimeZone.UTC)
    .parseDateTime(batchHour)

  def fromISO8601Hour(iso8601Hour: String): DateTime = iso8601Formatter.withZone(DateTimeZone.UTC)
    .parseDateTime(iso8601Hour)


  def getHourlyBatchPartition(basePath: String, batchHour: DateTime): String = s"$basePath/datehour=%d-%02d-%02dT%02d"
    .format(
      batchHour.getYear,
      batchHour.getMonthOfYear,
      batchHour.getDayOfMonth,
      batchHour.getHourOfDay)

}
