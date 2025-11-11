package pro.datawiki.diMigration.output.traits

trait TargetDag {
  def getDagName: String
  def getTaskPipelines: List[TargetTask]
}
