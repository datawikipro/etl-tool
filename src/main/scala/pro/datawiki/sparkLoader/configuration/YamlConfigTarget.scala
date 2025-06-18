package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.{YamlConfigTargetColumn, YamlConfigTargetDatabase, YamlConfigTargetFileSystem, YamlConfigTargetMessageBroker}
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.connection.{DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.task.Context
import pro.datawiki.yamlConfiguration.LogicClass

case class YamlConfigTarget(database: YamlConfigTargetDatabase,
                            fileSystem: YamlConfigTargetFileSystem,
                            messageBroker: YamlConfigTargetMessageBroker,
                            ignoreError: Boolean
                           )extends LogicClass {

  def getLogic: YamlConfigTargetTrait = {
    super.getLogic(database, fileSystem,messageBroker) match
      case x: YamlConfigTargetTrait => return x
      case _ => throw Exception()
  }
  
  def writeTarget(): Boolean = {
    if ignoreError then {
      try {
        getLogic.writeTarget()
      } catch
        case e: Exception => {
          println(e.toString)
          return true
        }
    } else {
      getLogic.writeTarget()
    }
  }

}

