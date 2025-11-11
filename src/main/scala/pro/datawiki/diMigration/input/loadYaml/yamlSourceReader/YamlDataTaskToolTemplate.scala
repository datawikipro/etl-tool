package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.*

import scala.collection.mutable

trait YamlDataTaskToolTemplate {
  def getCoreTask: List[CoreTask]
  def isRunFromControlDag:Boolean
}

object YamlDataTaskToolTemplate {
  def apply(templateName: String, templateLocation: String, row: Map[String, String]): YamlDataTaskToolTemplate = {
    templateName match {
      case "Kafka" => return YamlDataOdsKafkaTemplate(templateLocation, row)
      case "OdsPostgresMirror" => return YamlDataOdsPostgresMirrorTemplate(templateLocation, row)
      case "OdsMongoDBMirror" => return YamlDataOdsMongoDBMirrorTemplate(templateLocation, row)
      case "OdsDebeziumPostgresTableMirror" => return YamlDataOdsDebeziumPostgresTableMirror(templateLocation, row)
      case "OdsDebeziumMongoTableMirror" => return YamlDataOdsDebeziumMongaTableMirror(templateLocation, row)
      case "odsBigQuery" => return YamlDataOdsBigQueryMirrorTemplate(templateLocation, row)
      case _ => throw UnsupportedOperationException("Unsupported template type")
    }
  }

}