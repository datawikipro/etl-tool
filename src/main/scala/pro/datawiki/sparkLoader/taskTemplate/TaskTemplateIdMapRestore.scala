package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.overwriteTable
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateIdMapRestore(sourceName: String,
                               connection: DatabaseTrait,
                               template: mutable.Map[String, TaskTemplateIdMapConfig]
                              ) extends TaskTemplate {
  val cache: TransformationCacheDatabase = TransformationCacheDatabase()

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    var str: List[String] = List.apply()
    var idMapSelect: List[String] = List.apply()
    var idMapJoin: List[String] = List.apply()
    template.foreach(i => {
      str = str.appended(s"""cast(${i._2.columnNames.mkString("!@#")} as String) as ${i._1}_ccd""".stripMargin)
      idMapSelect = idMapSelect.appended(s"""${i._1}.rk as ${i._1}_rk""")
      idMapJoin = idMapJoin.appended(s"""left join idmap.${i._2.domainName} ${i._1} on ${i._1}.ccd = temporary.${i._1}_ccd and ${i._1}.system_code = '${i._2.systemCode}'""")
    })
    val sql: String =
      s"""
         |select ${str.mkString(",\n       ")},
         |       src.*
         |  from ${sourceName} src
         |  """.stripMargin

    var df = SparkObject.spark.sql(sqlText = sql)
    LogMode.debugDF(df)
    cache.saveTable(DataFrameOriginal(df), overwriteTable,connection)

    var sql2 =
      s"""select ${idMapSelect.mkString(",\n       ")},
         |       temporary.*
         |  from ${cache.getLocation} temporary
         |  ${idMapJoin.mkString("\n  ")}""".stripMargin
    val df2 = connection.getDataFrameBySQL(sql2)
    return List.apply(DataFrameOriginal(df2))
  }

}
