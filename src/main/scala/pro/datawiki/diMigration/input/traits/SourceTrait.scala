package pro.datawiki.diMigration.input.traits

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.core.task.CoreTask

trait SourceTrait {

  def getDagConfigs: List[CoreDag]
}
