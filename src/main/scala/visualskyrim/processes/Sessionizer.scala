package visualskyrim.processes

import org.joda.time.DateTime
import visualskyrim.common.AppConf
import visualskyrim.schema.{Normalized, SessionizationResult, Sessionized}

object Sessionizer {

  private val appConf = AppConf()
  private val MAX_DURATION = appConf.sessionMaxDuration
  private val SESSION_TIMEOUT = appConf.sessionTimeout

  case class OngoingSession(lastAccessTs: Int = 0, currentDuration: Int = 0, accesses: Seq[Normalized] = Seq.empty)
  case class Sessionization(sessions: Seq[Sessionized] = Seq.empty, ongoingSession: OngoingSession = OngoingSession())


  def sessionize(accesses: Seq[Normalized], batchHour: DateTime): SessionizationResult = {
    val sessionization =
      accesses
        .sortBy(x => x.timestamp)
        .foldLeft(Sessionization()) { (currSessionization, access) =>
          if (currSessionization.ongoingSession.lastAccessTs == 0) { // Initial case
            Sessionization(currSessionization.sessions, OngoingSession(access.timestamp, 0, Seq(access)))
          } else {
            val ongoingSession = currSessionization.ongoingSession

            if (access.timestamp - ongoingSession.lastAccessTs + ongoingSession.currentDuration > MAX_DURATION || // cut because of max duration
              access.timestamp - ongoingSession.lastAccessTs > SESSION_TIMEOUT) { // cut because of timeout
              val cutSession = toSessionized(ongoingSession)

              Sessionization(
                currSessionization.sessions :+ cutSession,
                OngoingSession(access.timestamp, 0, Seq(access))
              )
            } else {
              Sessionization(
              currSessionization.sessions,
                OngoingSession(
                  access.timestamp,
                  access.timestamp - ongoingSession.lastAccessTs + ongoingSession.currentDuration,
                  ongoingSession.accesses :+ access
                )
              )
            }

            // TODO: Possibly, it is better to add one more cut rule: cut when there are 5000 accesses
          }
        }

    // Check if the final ongoing session can be cut in this hour
    if (((batchHour.getMillis / 1000).toInt + 60 * 60) - sessionization.ongoingSession.accesses.last.timestamp > SESSION_TIMEOUT) {

      SessionizationResult(sessionization.sessions :+ toSessionized(sessionization.ongoingSession), Seq.empty)
    } else {
      SessionizationResult(sessionization.sessions, sessionization.ongoingSession.accesses)
    }
  }

  private def toSessionized(ongoingSession: OngoingSession): Sessionized = {
    Sessionized(
      ongoingSession.accesses.head.clientId,
      ongoingSession.currentDuration,
      ongoingSession.accesses.size,
      ongoingSession.accesses.map(_.url).toSet.size,
      ongoingSession.accesses.head.timestamp,
      ongoingSession.accesses.last.timestamp
    )
  }
}
