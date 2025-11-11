package pro.datawiki.diMigration.output.traits

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.core.dictionary.OutputSystem
import pro.datawiki.diMigration.output.etlTool.{EtlToolMigration, OutputSql}

trait TargetMigration {
  def exportDag(in: CoreDag): TargetDag
  def getTargetLocation:String
}

object TargetMigration {
  def apply(configName: OutputSystem, location: String, templateLocation: String): TargetMigration = {
    configName match
      case OutputSystem.EtlTool => return EtlToolMigration(location, templateLocation)
      case OutputSystem.Sql => return OutputSql(location, templateLocation)
      case _ => throw UnsupportedOperationException("Unsupported output system type")
  }
}