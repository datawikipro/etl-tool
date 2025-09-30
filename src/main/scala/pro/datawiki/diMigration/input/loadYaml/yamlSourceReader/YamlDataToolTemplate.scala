package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreDag}
import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.{YamlDataOdsKafkaTemplate, YamlDataOdsMongoDBMirrorTemplate, YamlDataOdsPostgresMirrorTemplate}

import scala.collection.mutable

trait YamlDataToolTemplate {
  def getCoreDag: List[CoreDag]
}