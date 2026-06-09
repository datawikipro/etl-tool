package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateReadEmail, TaskTemplateSQLFromDatabase}

import java.util.{Calendar, Date}

case class YamlConfigSourceMail(
                                 email: String,
                                 password: String,
                                 from: String,
                                 subject: String,
                               ) extends YamlConfigSourceTrait {


  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    return TaskTemplateReadEmail(email, password, from, subject, connection)
  }
}