package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource

import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateReadEmail, TaskTemplateSQLFromDatabase}

import java.util.{Calendar, Date}

case class YamlDataTemplateSourceMail(
                                       email: String,
                                       password: String,
                                       from: String,
                                       subject: String,
                                     )