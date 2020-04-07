package visualskyrim.schema

case class SessionizationResult(sessions: Seq[Sessionized] = Seq.empty,
                                watermark: SessionCutWatermark = SessionCutWatermark())
