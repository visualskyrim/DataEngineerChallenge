package visualskyrim.processes

import org.joda.time.DateTime
import visualskyrim.common.AppConf
import visualskyrim.processes.Sessionizer.toSessionized
import visualskyrim.schema.{Normalized, SessionCutWatermark, SessionizationResult, Sessionized}

object Sessionizer {

  private val appConf = AppConf()
  private val MAX_DURATION = appConf.sessionMaxDuration
  private val SESSION_TIMEOUT = appConf.sessionTimeout


  case class SessionizationWithWatermark(sessions: Seq[Sessionized] = Seq.empty, watermark: SessionCutWatermark = SessionCutWatermark())

  def sessionize(accesses: Seq[Normalized], watermark: SessionCutWatermark, batchHour: DateTime): SessionizationResult = {
    val sessionization = accesses
      .sortBy(x => x.timestamp)
      .foldLeft(SessionizationWithWatermark(Seq.empty, watermark)) { (currSessionization, access) =>
        if (currSessionization.watermark.clientId.isEmpty) { // Initial case
          SessionizationWithWatermark(
            currSessionization.sessions,
            SessionCutWatermark(access.clientId, access.timestamp, access.timestamp, 0, 1, Set(access.url)))
        } else {
          val watermark = currSessionization.watermark

          if (access.timestamp - watermark.lastAccessTs + watermark.currentDuration > MAX_DURATION || // cut because of max duration
            access.timestamp - watermark.lastAccessTs > SESSION_TIMEOUT) { // cut because of timeout
            val cutSession = toSessionized(watermark)

            SessionizationWithWatermark(
              currSessionization.sessions :+ cutSession,
              SessionCutWatermark(access.clientId, access.timestamp, access.timestamp, 0, 1, Set(access.url))
            )
          } else {
            SessionizationWithWatermark(
              currSessionization.sessions,
              SessionCutWatermark(access.clientId, watermark.firstAccessTs, access.timestamp,
                access.timestamp - watermark.firstAccessTs, watermark.currentAccesses + 1, watermark.urls + access.url)
            )
          }

          // TODO: Possibly, it is better to add one more cut rule: cut when there are 5000 accesses to further avoid bot-like sessions
        }
      }

    // Check if the final ongoing session can be cut in this hour
    if (((batchHour.getMillis / 1000).toInt + 60 * 60) - sessionization.watermark.lastAccessTs > SESSION_TIMEOUT) {
      SessionizationResult(sessionization.sessions :+ toSessionized(sessionization.watermark), SessionCutWatermark())
    } else {
      SessionizationResult(sessionization.sessions, sessionization.watermark)
    }
  }


  private def toSessionized(watermark: SessionCutWatermark): Sessionized = {
    Sessionized(
      watermark.clientId,
      watermark.currentDuration,
      watermark.currentAccesses,
      watermark.urls.size,
      watermark.firstAccessTs,
      watermark.lastAccessTs
    )
  }
}
