package pro.datawiki.sparkLoader.register

import pro.datawiki.sparkLoader.traits.LoggingTrait

object TableRegister extends LoggingTrait {
  def apply(config: Option[YamlConfigRegister]): Option[TableRegisterTrait] = {
    config.flatMap { cfg =>
      cfg.registerType.toLowerCase match {
        case "trino-jdbc" =>
          val url = cfg.url.getOrElse("")
          val user = cfg.user.getOrElse("chernousov_a")
          if (url.nonEmpty) {
            Some(new TrinoJdbcTableRegister(url, user))
          } else {
            logWarning("Trino JDBC registry URL is empty, skipping registration")
            None
          }
        case other =>
          logWarning(s"Unknown register type: $other")
          None
      }
    }
  }
}
