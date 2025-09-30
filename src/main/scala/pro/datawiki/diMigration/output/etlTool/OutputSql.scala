package pro.datawiki.diMigration.output.etlTool

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.output.traits.TargetMigration
import pro.datawiki.exception.NotImplementedException

class OutputSql(targetLocation: String, templateLocation: String) extends TargetMigration {

  override def exportDag(in: CoreDag): Unit = {
    throw NotImplementedException("SQL output export functionality not implemented")
  }
}
