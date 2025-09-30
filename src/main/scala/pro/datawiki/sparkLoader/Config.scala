package pro.datawiki.sparkLoader

case class Config(
                   configLocation: String ="",
                   partition: String = "",
                   runId: String = "",
                   isDebug: Boolean = false,
                   externalProgressLogging: Option[ExternalProgressLoggingConfig] = None,
                   dagName: String = "not defined",
                   taskName: String = "not defined",
                   loadDate: String = "not defined"
                 )
