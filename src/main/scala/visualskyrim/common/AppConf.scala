package visualskyrim.common

import com.typesafe.config.{Config, ConfigFactory}

case class AppConf(input: String, sessionized: String, pending: String, error: String, sessionTimeout: Int, sessionMaxDuration: Int)

object AppConf {
  private val cfg = ConfigFactory.load()

  def apply(): AppConf = {
     cfg.getString("data.input")

    new AppConf(cfg.getString("data.input"),
      cfg.getString("data.sessionized"),
      cfg.getString("data.pending"),
      cfg.getString("data.error"),
      cfg.getInt("session.timeout"),
      cfg.getInt("session.maxDuration"))
  }
}
