package visualskyrim.processes

import org.joda.time.DateTime
import org.scalatest.FunSuite
import visualskyrim.schema.Normalized

class SessionizerSuite extends FunSuite {

  test("Cut by timeout") {
    val batchTime = new DateTime(2020, 4, 6, 0, 0)
    val batchTimeInTs = (batchTime.getMillis / 1000) toInt

    val testingSeq = Seq(
      Normalized(batchTimeInTs + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 2, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 3, "1.1.1.1:0", "", "url2", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 4, "1.1.1.1:0", "", "url3", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 5 + 20 * 60 + 1, "1.1.1.1:0", "", "url2", "1.1.1.1:0"),// <--- should cut here
      Normalized(batchTimeInTs + 6 + 20 * 60 + 1, "1.1.1.1:0", "", "url2", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 7 + 20 * 60 + 1, "1.1.1.1:0", "", "url3", "1.1.1.1:0")
    )

    val sessionizedResult = Sessionizer.sessionize(testingSeq, batchTime)

    assert(sessionizedResult.pending.size === 0)
    assert(sessionizedResult.sessions.size === 2)
    assert(sessionizedResult.sessions(0).accesses === 4)
    assert(sessionizedResult.sessions(0).duration === 3)
    assert(sessionizedResult.sessions(0).uniqUrls === 3)
    assert(sessionizedResult.sessions(1).accesses === 3)
    assert(sessionizedResult.sessions(1).duration === 2)
    assert(sessionizedResult.sessions(1).uniqUrls === 2)
  }

  test("Cut by timeout with disordered accesses") {
    val batchTime = new DateTime(2020, 4, 6, 0, 0)
    val batchTimeInTs = (batchTime.getMillis / 1000) toInt

    val testingSeq = Seq(
      Normalized(batchTimeInTs + 2, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 5 + 20 * 60 + 1, "1.1.1.1:0", "", "url1","1.1.1.1:0"),// <--- should cut here
      Normalized(batchTimeInTs + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 4, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 6 + 20 * 60 + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 3, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 7 + 20 * 60 + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0")
    )

    val sessionizedResult = Sessionizer.sessionize(testingSeq, batchTime)

    assert(sessionizedResult.pending.size === 0)
    assert(sessionizedResult.sessions.size === 2)
    assert(sessionizedResult.sessions(0).accesses === 4)
    assert(sessionizedResult.sessions(0).duration === 3)
    assert(sessionizedResult.sessions(1).accesses === 3)
    assert(sessionizedResult.sessions(1).duration === 2)
  }

  test("Cut by timeout with pending") {
    val batchTime = new DateTime(2020, 4, 6, 0, 0)
    val batchTimeInTs = (batchTime.getMillis / 1000) toInt

    val testingSeq = Seq(
      Normalized(batchTimeInTs + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 2, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 3, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 4, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 5 + 40 * 60 + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),// <--- should cut here
      Normalized(batchTimeInTs + 6 + 40 * 60 + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0"),
      Normalized(batchTimeInTs + 7 + 40 * 60 + 1, "1.1.1.1:0", "", "url1", "1.1.1.1:0")
    )

    val sessionizedResult = Sessionizer.sessionize(testingSeq, batchTime)

    assert(sessionizedResult.pending.size === 3)
    assert(sessionizedResult.sessions.size === 1)
    assert(sessionizedResult.sessions(0).accesses === 4)
    assert(sessionizedResult.sessions(0).duration === 3)
  }


  test("Cut by max duration") {
    val batchTime = new DateTime(2020, 4, 6, 0, 0)
    val batchTimeInTs = (batchTime.getMillis / 1000) toInt

    val testingSeq = (1 to 26) map (x => Normalized(batchTimeInTs - 6 * 60 * 60 + x * 15 * 60, "1.1.1.1:0", "", "url1", "1.1.1.1:0"))


    val sessionizedResult = Sessionizer.sessionize(testingSeq, batchTime)

    assert(sessionizedResult.pending.size === 0)
    assert(sessionizedResult.sessions.size === 2)
    assert(sessionizedResult.sessions(0).accesses === 25)
    assert(sessionizedResult.sessions(0).duration === 6 * 60 * 60)

    assert(sessionizedResult.sessions(1).accesses === 1)
    assert(sessionizedResult.sessions(1).duration === 0)
  }


  test("Cut by max duration with pending") {
    val batchTime = new DateTime(2020, 4, 6, 0, 0)
    val batchTimeInTs = (batchTime.getMillis / 1000) toInt

    val testingSeq = (1 to 27) map (x => Normalized(batchTimeInTs - 6 * 60 * 60 + x * 15 * 60, "1.1.1.1:0", "", "url1", "1.1.1.1:0"))


    val sessionizedResult = Sessionizer.sessionize(testingSeq, batchTime)

    assert(sessionizedResult.pending.size === 2)
    assert(sessionizedResult.sessions.size === 1)
    assert(sessionizedResult.sessions(0).accesses === 25)
    assert(sessionizedResult.sessions(0).duration === 6 * 60 * 60)
  }


}
