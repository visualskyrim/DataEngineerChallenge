package visualskyrim.schema


case class SessionCutWatermark(clientId: String = "", firstAccessTs: Int = 0, lastAccessTs: Int = 0,
                               currentDuration: Int = 0, currentAccesses: Int = 0, urls: Set[String] = Set.empty)