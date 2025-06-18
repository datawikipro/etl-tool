package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.task.{Task, TaskTemplate}

trait YamlConfigTargetTrait {
  def writeTarget(): Boolean
}
