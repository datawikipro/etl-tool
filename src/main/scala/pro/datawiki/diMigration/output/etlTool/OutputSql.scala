package pro.datawiki.diMigration.output.etlTool

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.output.traits.TargetMigration

class OutputSql(targetLocation: String,templateLocation:String) extends TargetMigration {

  override def exportDag(in: CoreDag): Unit= {
    throw Exception()
  }
}
