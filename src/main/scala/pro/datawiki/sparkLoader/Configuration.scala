package pro.datawiki.sparkLoader

import scopt.OptionParser
import org.joda.time.DateTime

private object Configuration {
  val jobClassName: String = this.getClass.getName
  val optionParser: OptionParser[Config] = new OptionParser[Config](jobClassName) {
    opt[String]("executionDate")           .action { (ed, config) => {assert(ed != "", "executionDate is empty" );config.copy(executionDate = DateTime.parse(ed))}}.text("Start datetime in format YYYY-MM-DDTHH:mm:ss, will be truncated to date")
    arg[String]("configLocation")          .action { (mp, config) => {assert(mp != "", "configLocation is empty");config.copy(configLocation= mp                )}}.text("configLocation")
    arg[String]("jobName")      .optional().action { (jn, config) =>                                              config.copy(jobName       = jn)                 }.text("Job name")
  }

  case class Config(
                     jobName: String = jobClassName + ".debug",
                     executionDate: DateTime = DateTime.now(),
                     configLocation: String = s"./META.yaml",
                     broadcastTimeout: Int = -1
                   ) {
    val executionDateStr: String = executionDate.toString("yyyy-MM-dd")
    val configLocationStr: String = configLocation
  }

}
