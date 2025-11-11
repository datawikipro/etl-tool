package pro.datawiki.diMigration.core.dag

import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.output.traits.TargetMigration
import pro.datawiki.exception.NotImplementedException

import java.util.Date

case class CoreControlDag(
                        dagName: String,
                        tags: List[String] = List.empty,
                        dagDescription: String,
                        schedule: Schedule = Schedule.None,
                        startDate: Date,
                        catchup: Boolean,
                        retries: Int,
                        pythonFile: String,
                        taskPipelines: List[(List[CoreTask], Boolean)] = List.empty
                      ) extends CoreDag {
  override def ttt: Unit = throw NotImplementedException("CoreDag.ttt method not implemented")
}
