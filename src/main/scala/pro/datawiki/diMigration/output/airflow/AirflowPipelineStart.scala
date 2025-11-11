package pro.datawiki.diMigration.output.airflow

import pro.datawiki.diMigration.core.task.CoreTask

case class AirflowPipelineStart(dagName:String,taskName: String)extends CoreTask {
  override def getTaskId: String = {
    return taskName
  }
}
