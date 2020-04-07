package visualskyrim.processes

import org.joda.time.DateTime
import org.scalatest.FunSuite
import visualskyrim.schema.{Normalized, SessionCutWatermark}

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

    val sessionizedResult = Sessionizer.sessionize(testingSeq, SessionCutWatermark(), batchTime)

    assert(sessionizedResult.watermark === SessionCutWatermark())
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

    val sessionizedResult = Sessionizer.sessionize(testingSeq, SessionCutWatermark(), batchTime)

    assert(sessionizedResult.watermark === SessionCutWatermark())
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

    val sessionizedResult = Sessionizer.sessionize(testingSeq, SessionCutWatermark(), batchTime)

    assert(sessionizedResult.watermark.currentAccesses === 3)
    assert(sessionizedResult.watermark.currentDuration === 2)
    assert(sessionizedResult.watermark.firstAccessTs === batchTimeInTs + 5 + 40 * 60 + 1)
    assert(sessionizedResult.watermark.lastAccessTs === batchTimeInTs + 7 + 40 * 60 + 1)
    assert(sessionizedResult.watermark.urls.size === 1)
    assert(sessionizedResult.sessions.size === 1)
    assert(sessionizedResult.sessions(0).accesses === 4)
    assert(sessionizedResult.sessions(0).duration === 3)
  }


  test("Cut by max duration") {
    val batchTime = new DateTime(2020, 4, 6, 0, 0)
    val batchTimeInTs = (batchTime.getMillis / 1000) toInt

    val testingSeq = (1 to 2) map (x => Normalized(batchTimeInTs + x * 15 * 60, "1.1.1.1:0", "", "url1", "1.1.1.1:0"))


    val sessionizedResult = Sessionizer.sessionize(
      testingSeq,
      SessionCutWatermark(
        "1.1.1.1:0",
        batchTimeInTs + 1 * 15 * 60 - 6 * 60 * 60,
        batchTimeInTs - 1,
        batchTimeInTs - 1 - (batchTimeInTs + 1 * 15 * 60 - 6 * 60 * 60),
        30, Set("url1")),
      batchTime)

    assert(sessionizedResult.watermark === SessionCutWatermark())
    assert(sessionizedResult.sessions.size === 2)
    assert(sessionizedResult.sessions(0).accesses === 31)
    assert(sessionizedResult.sessions(0).duration === 6 * 60 * 60)

    assert(sessionizedResult.sessions(1).accesses === 1)
    assert(sessionizedResult.sessions(1).duration === 0)
  }


  test("Cut by max duration with pending") {
    val batchTime = new DateTime(2020, 4, 6, 0, 0)
    val batchTimeInTs = (batchTime.getMillis / 1000) toInt

    val testingSeq = (1 to 3) map (x => Normalized(batchTimeInTs + x * 15 * 60, "1.1.1.1:0", "", "url1", "1.1.1.1:0"))


    val sessionizedResult = Sessionizer.sessionize(
      testingSeq,
      SessionCutWatermark(
        "1.1.1.1:0",
        batchTimeInTs + 1 * 15 * 60 - 6 * 60 * 60,
        batchTimeInTs - 1,
        batchTimeInTs - 1 - (batchTimeInTs + 1 * 15 * 60 - 6 * 60 * 60),
        30, Set("url1")),
      batchTime)

    assert(sessionizedResult.sessions.size === 1)
    assert(sessionizedResult.sessions(0).accesses === 31)
    assert(sessionizedResult.sessions(0).duration === 6 * 60 * 60)

    assert(sessionizedResult.watermark.lastAccessTs === batchTimeInTs + 3 * 15 * 60)
    assert(sessionizedResult.watermark.firstAccessTs === batchTimeInTs + 2 * 15 * 60)
    assert(sessionizedResult.watermark.currentDuration === 1 * 15 * 60)
    assert(sessionizedResult.watermark.currentAccesses === 2)
    assert(sessionizedResult.watermark.urls === Set("url1"))
  }


}
