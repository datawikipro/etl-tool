package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.{Connection,DataWarehouseTrait, FileStorageTrait, WriteMode}

case class YamlConfigTarget(connection: String,
                            source: String,
                            targetFile: String,
                            autoInsertIdmapCCD: Boolean,
                            columns: List[YamlConfigTargetColumn],
                            uniqueKey: List[String] = List.apply(),
                            mode: String = "append",
                            partitionBy: List[String] = List.apply()
                           ) {
  private def getMode: WriteMode = {
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

  def writeTarget(): Boolean = {
    val df = SparkObject.spark.sql(s"select * from $source")
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }

    Connection.getConnection(connection) match
      case x: FileStorageTrait => {
        if partitionBy.nonEmpty then {
          x.writeDfPartitionAuto(df, targetFile, partitionBy, getMode)
          return true
        }

        RunConfig.getPartition match
          case null => {
            x.writeDf(df, targetFile, getMode)
            return true
          }
          case _ => x.writeDfPartitionDirect(df, targetFile, partitionBy, List.apply(RunConfig.getPartition), getMode)
            return true

      }
      case x: DataWarehouseTrait => {
        if uniqueKey.nonEmpty then {
          x.writeDf(df, targetFile, uniqueKey, getColumns, getMode)
          return true
        }
        x.writeDf(df, targetFile, getMode)
        return true
      }
      case _ => throw Exception()
  }

}

