package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource

import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateSQLFromDatabase}

case class YamlDataTemplateSourceDBSQL(
                                        sql: String,
                                      )