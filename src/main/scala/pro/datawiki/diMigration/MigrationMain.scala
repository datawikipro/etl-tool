package pro.datawiki.diMigration

import pro.datawiki.diMigration.traits.LoggingTrait

object MigrationMain extends LoggingTrait {

  @main
  def main(configLocation: String): Unit = {
    val startTime = logOperationStart("migration process", s"config: $configLocation")

    try {
      val sourceConfig: AttributeYaml = AttributeYaml(configLocation)

      val results = sourceConfig.transformations.map { transformation => transformation.process() }

    } catch {
      case e: Exception =>
        logError("migration process", e, "fatal error")
        System.exit(1)
    }
  }
}
