package pro.datawiki.diMigration.core.dag

import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.output.traits.TargetMigration

case class CoreBaseDag(
                   dagName:String,
                   tags: List[String]= List.apply(),
                   dagDescription: String,
                   schedule: Schedule=Schedule.None,
                   pythonFile: String,
                   listTask: List[CoreTask] =List.apply()
                 ) extends CoreDag {
}
