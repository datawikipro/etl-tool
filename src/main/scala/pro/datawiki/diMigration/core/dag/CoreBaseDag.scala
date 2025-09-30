package pro.datawiki.diMigration.core.dag

import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.output.traits.TargetMigration

import java.util.Date

case class CoreBaseDag(
                        dagName: String,
                        tags: List[String] = List.apply(),
                        dagDescription: String,
                        schedule: Schedule = Schedule.None,
                        startDate: Date,
                        catchup: Boolean,
                        pythonFile: String,
                        taskPipelines: List[List[CoreTask]] = List.apply()
                      ) extends CoreDag {
}
