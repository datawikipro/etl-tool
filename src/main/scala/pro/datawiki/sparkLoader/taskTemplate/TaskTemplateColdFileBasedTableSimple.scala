package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerColdDataFileBasedFolder
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetFileSystem
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}

class TaskTemplateColdFileBasedTableSimple(source: YamlConfigEltOnServerColdDataFileBasedFolder,
                                           target: YamlConfigEltOnServerColdDataFileBasedFolder,
                                           connection: ConnectionTrait) extends TaskTemplate {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] =
    throw NotImplementedException("Method not implemented")
}