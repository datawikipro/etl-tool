package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameLazySparkSql, DataFrameOriginal, DataFramePartition, DataFrameTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateSparkSql(sql: String,
                           isLazyTransform: Boolean = false,
                           lazyTable: List[String] = List.empty) extends TaskTemplate {

  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    SparkObject.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    if !isLazyTransform then {
      lazyTable.length match
        case 0 =>
        case 1 => throw Exception()
        case _ => throw Exception()

      val df: DataFrame = SparkObject.spark.sql(sql)
      LogMode.debugString(sql)
      LogMode.debugDF(df)
      return List.apply(DataFrameOriginal(df))
    }

    lazyTable.length match
      case 0 =>
        return List.apply(DataFrameLazySparkSql(sql, mutable.Map()))
      case 1 => {
        var list: mutable.Map[String,DataFrameTrait] = mutable.Map()
        val view_name = lazyTable.head
        val view = Task.getView(view_name)
        view match
          case x: DataFramePartition => {
            x.getPartitions.foreach(i=> {
              list += (i._1 -> DataFrameLazySparkSql(sql=sql, inInitTables = mutable.Map((view_name->i._2))))
            })
          }
          case _ => throw Exception()
        return List.apply(DataFramePartition(list))
      }
      case _=>
        throw Exception()
  }

}
