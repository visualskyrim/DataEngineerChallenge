package visualskyrim.schema

case class SessionizationResult(sessions: Seq[Sessionized] = Seq.empty, pending: Seq[Normalized] = Seq.empty)
