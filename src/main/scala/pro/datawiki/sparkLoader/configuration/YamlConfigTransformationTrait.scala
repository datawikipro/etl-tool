package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.task.{Task, TaskTemplate}

trait YamlConfigTransformationTrait {
  def getTaskTemplate: TaskTemplate

  def getTask(in: TaskTemplate): Task
}
