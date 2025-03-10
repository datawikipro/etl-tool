package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.{DataWarehouseTrait, WriteMode}
import pro.datawiki.sparkLoader.target.Target

case class YamlConfigTarget(connection: String,
                            targetFile: String,
                            autoInsertIdmapCCD: Boolean,
                            columns: List[YamlConfigTargetColumn],
                            uniqueKey: List[String] = List.apply(),
                            mode: String = "append",
                            partitionBy: List[String] = List.apply()
                           ) {
  def getMode: WriteMode = {
    mode match
      case "overwrite" => WriteMode.overwrite
      case "append" => WriteMode.append
      case _ => throw Exception()
  }

  def getColumns: List[String] = {
    var list: List[String] = List.apply()
    columns.foreach(i => list = list.appended(i.columnName))
    return list diff uniqueKey
  }

  def getPartitionBy: List[String] = {
    partitionBy
  }

  def writeTarget(): Unit = {
    val df = SparkObject.spark.sql("select * from target")
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    Target.getTarget match
      case x: DataWarehouseTrait =>
        if uniqueKey.nonEmpty then
          x.writeDf(df, targetFile, uniqueKey, getColumns, getMode)
          return
        else {
          if partitionBy.nonEmpty then {
            x.writeDfPartitionAuto(df, targetFile, partitionBy, getMode)
            return
          }
          RunConfig.getPartition match
            case "All" => x.writeDf(df, s"${targetFile}", getMode)
            case _ => x.writeDfPartitionDirect(df, targetFile, partitionBy, List.apply(RunConfig.getPartition), getMode)
            return
        }
      case _ => throw Exception()
  }

}

