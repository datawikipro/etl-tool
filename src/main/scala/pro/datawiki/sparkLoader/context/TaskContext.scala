package pro.datawiki.sparkLoader.context

import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate

import scala.collection.mutable

object TaskContext {

  private var taskTemplate: mutable.Map[String, TaskTemplate] = mutable.Map()

  def setTaskTemplate(name: String, task: TaskTemplate): Unit = {
    taskTemplate += (name, task)
  }

  def getTaskTemplate(in: String): TaskTemplate = {
    try {
      return taskTemplate(in)
    } catch
      case _ =>
        throw ConfigurationException(s"Task template '$in' not initialized. Make sure it's properly configured.")
  }

}
