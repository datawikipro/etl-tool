package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.connection.minIo.minioIceberg.LoaderMinIoIceberg
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.overwriteTable
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateIdMapRestore(sourceName: String,
                               connection: ConnectionTrait,
                               template: mutable.Map[String, TaskTemplateIdMapConfig]
                              ) extends TaskTemplate {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    var str: List[String] = List.apply()
    var idMapSelect: List[String] = List.apply()
    template.foreach(i => {
      str = str.appended(s"""cast(${i._2.columnNames.mkString("!@#")} as String) as ${i._1}_ccd""".stripMargin)
      idMapSelect = idMapSelect.appended(s"""${i._1}.rk as ${i._1}_rk""")
    })
    val sql: String =
      s"""
         |select ${str.mkString(",\n       ")},
         |       src.*
         |  from ${sourceName} src
         |  """.stripMargin

    var df = SparkObject.spark.sql(sqlText = sql)
    LogMode.debugDF(df)

    connection match {
      case db: DatabaseTrait =>
        val cache = TransformationCacheDatabase()
        cache.saveTable(DataFrameOriginal(df), overwriteTable, db)
        var idMapJoin: List[String] = List.apply()
        template.foreach(i => {
          idMapJoin = idMapJoin.appended(s"""left join idmap.${i._2.domainName} ${i._1} on ${i._1}.ccd = temporary.${i._1}_ccd and ${i._1}.system_code = '${i._2.systemCode}'""")
        })
        var sql2 =
          s"""select ${idMapSelect.mkString(",\n       ")},
             |       temporary.*
             |  from ${cache.getLocation} temporary
             |  ${idMapJoin.mkString("\n  ")}""".stripMargin
        val df2 = db.getDataFrameBySQL(sql2)
        List.apply(DataFrameOriginal(df2))

      case fs: LoaderMinIoIceberg =>
        val tempViewName = "tmp_restore_" + scala.util.Random.alphanumeric.filter(_.isLetter).take(10).mkString
        df.createOrReplaceTempView(tempViewName)
        var idMapJoin: List[String] = List.apply()
        template.foreach(i => {
          val resolvedLocation = i._2.getResolvedLocation
          val resolvedTable = fs.fullRef(resolvedLocation)

          if (!SparkObject.spark.catalog.tableExists(resolvedTable)) {
            fs.createSchemaIfNotExists(resolvedLocation)
            SparkObject.spark.sql(
              s"""CREATE TABLE $resolvedTable (
                 |  ccd STRING,
                 |  source_code STRING,
                 |  rk BIGINT
                 |) USING iceberg
                 |TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                 |""".stripMargin
            )
          }

          idMapJoin = idMapJoin.appended(s"""left join $resolvedTable ${i._1} on ${i._1}.ccd = temporary.${i._1}_ccd and ${i._1}.source_code = '${i._2.systemCode}'""")
        })
        var sql2 =
          s"""select ${idMapSelect.mkString(",\n       ")},
             |       temporary.*
             |  from ${tempViewName} temporary
             |  ${idMapJoin.mkString("\n  ")}""".stripMargin
        val df2 = SparkObject.spark.sql(sql2)
        List.apply(DataFrameOriginal(df2))

      case _ =>
        throw UnsupportedOperationException(s"Unsupported connection type for ID map restore: ${connection.getClass.getSimpleName}")
    }
  }

}
