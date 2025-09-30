package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap.{YamlConfigTransformationIdMapConfig, YamlConfigTransformationIdMapMerge}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.*

import scala.collection.mutable

case class YamlConfigTransformationIdMap(
                                          sourceName: String = throw IllegalArgumentException("sourceName is required"),
                                          connection: String = throw IllegalArgumentException("connection is required"),
                                          idMapGenerate: List[YamlConfigTransformationIdMapConfig] = List.apply(),
                                          idMapRestore: List[YamlConfigTransformationIdMapConfig] = List.apply(),
                                          idMapMerge: List[YamlConfigTransformationIdMapMerge] = List.apply()
                                        ) extends YamlConfigTransformationTrait {
  //  override def getDataFrame: DataFrame = {
  //    val connect= Connection.getConnection(connection)
  //    var df = SparkObject.spark.sql(s"select * from ${sourceName}")
  //    LogMode.debugDF(df)
  //    idmaps.foreach(j => {
  //      df = j.addendNewKeys(df,connect)
  //      LogMode.debugDF(df)
  //    })
  //    return df
  //  }
  //  override def getTask(in: TaskTemplate): Task = throw NotImplementedException("Method not implemented")

  var locConnection: DatabaseTrait = null

  private def getConnection: DatabaseTrait = {
    if locConnection == null then {
      ApplicationContext.getConnection(connection) match
        case x: DatabaseTrait => locConnection = x
        case _ => throw UnsupportedOperationException("Unsupported connection type for ID map transformation")
    }
    return locConnection
  }


  override def getTaskTemplate: TaskTemplate = {
    var list: List[TaskTemplate] = List.apply()
    var listRestore: mutable.Map[String, TaskTemplateIdMapConfig] = mutable.Map()

    idMapGenerate.foreach(i => {
      list = list.appended(TaskTemplateIdMapGenerate(sourceName = sourceName, connection = getConnection, i.getTaskTemplateIdMapConfig))
      listRestore += (i.getAlias -> i.getTaskTemplateIdMapConfig)
    })

    idMapRestore.foreach(i => {
      listRestore += (i.getAlias -> i.getTaskTemplateIdMapConfig)
    })

    idMapMerge.foreach(i => {
      list = list.appended(TaskTemplateIdMapMerge(
        sourceName = sourceName,
        connection = getConnection,
        in = i.getIn.getTaskTemplateIdMapConfig,
        out = i.getOut.getTaskTemplateIdMapConfig))

      if i.getRestoreRk then listRestore += (i.getAlias -> i.getTaskTemplateIdMapConfig)
    })

    return TaskTemplateIdMap(TaskTemplateIdMapRestore(sourceName = sourceName, connection = getConnection, template = listRestore), list)
  }


  override def getTask(in: TaskTemplate): Task = TaskSimple(in)
}
