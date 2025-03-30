package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.{Connection, DataWarehouseTrait, FileStorageTrait, WriteMode}

case class YamlConfigTarget(connection: String,
                            source: String,
                            targetFile: String,
                            autoInsertIdmapCCD: Boolean,
                            columns: List[YamlConfigTargetColumn],
                            uniqueKey: List[String] = List.apply(),
                            mode: String = "append",
                            partitionBy: List[String] = List.apply(),
                            deduplicationKey: List[String] = List.apply(),
                            ignoreError: Boolean = false
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
    try {
      val sql = deduplicationKey.nonEmpty match
        case true => s"""with a as (select *, row_number() over (partition by ${uniqueKey.mkString(",")} order by ${deduplicationKey.mkString(",")}) as rn from $source) select * from a where rn = 1"""
        case false => s"select * from $source"

      val df = SparkObject.spark.sql(sql)
      LogMode.debugDF(df)

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
    } catch
      case e: Exception => {
        if ignoreError then {
          println(e.toString)
          return true
        } else {
          throw e
        }
      }
  }

}

