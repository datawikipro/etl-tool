package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFramePartition, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

case class YamlConfigTargetDatabase(
                                     connection: String,
                                     source: String,
                                     mode: String = "append",
                                     partitionMode: String,

                                     targetTable: String,
                                     columns: List[YamlConfigTargetColumn],
                                     uniqueKey: List[String] = List.apply(),
                                     deduplicationKey: List[String] = List.apply(),

                                     partitionBy: List[String] = List.apply(),
                                   ) extends YamlConfigTargetBase(connection = connection, mode = mode, partitionMode = partitionMode, source = source), YamlConfigTargetTrait {

  private var locCache: TransformationCacheDatabase = null
  private var locAllFields: List[String] = List.empty
  private var locFieldsWithChanges: List[String] = List.empty
  private var locFieldsWithoutChanges: List[String] = List.empty

  def cache: TransformationCacheDatabase = {
    if locCache == null then locCache = new TransformationCacheDatabase(loader)
    return locCache
  }

  override def loader: DatabaseTrait = {
    super.loader match
      case x: DatabaseTrait => x
      case _ => 
        throw Exception()
  }

  private def targetColumns: List[String] = {
    if locAllFields.nonEmpty then return locAllFields

    val dfTarget = loader.readDfSchema(targetTable)
    dfTarget.schema.fields.foreach(i => locAllFields = locAllFields :+ i.name)

    return locAllFields
  }

  private def columnsWithChanges: List[String] = {
    if locFieldsWithChanges.nonEmpty then return locFieldsWithChanges
    var a: List[String] = List.apply()

    columns.foreach(i => {
      a = a.appended(i.columnName)
    })
    locFieldsWithChanges = a diff uniqueKey
    return locFieldsWithChanges
  }

  private def columnWithOutChanges: List[String] = {
    if locFieldsWithoutChanges.nonEmpty then return locFieldsWithoutChanges
    locFieldsWithoutChanges = targetColumns diff (uniqueKey ::: columnsWithChanges ::: List.apply("valid_from_dttm", "valid_to_dttm"))
    return locFieldsWithoutChanges
  }

  private def getJoinString: String = {
    var joinList: List[String] = List.apply()
    uniqueKey.foreach(i => {
      joinList = joinList.appended(s"src.${i} = tgt.${i}")
    })
    return joinList.mkString(" and ")
  }

  var extraFilter: String = ""

  private def getExtraFilter: String = {
    if extraFilter != "" then return extraFilter

    if partitionBy.nonEmpty then {
      val sql1 = s"""select min(${partitionBy.head}) as min_partition from ${cache.getLocation}"""
      val minPartition = loader.getDataFrameBySQL(sql1).collect().head.get(0).toString
      extraFilter = s"""and ${partitionBy.head} >= '${minPartition}'"""
    }

    return extraFilter
  }

  private def calDeltaTable(): Boolean = {
    val sql12: String =
      s"""select ${(uniqueKey ::: columnsWithChanges).mkString(",")} from ${cache.getLocation}
         |except
         |select ${(uniqueKey ::: columnsWithChanges).mkString(",")} from ${targetTable} where valid_to_dttm = to_date('2100','yyyy') ${getExtraFilter}
         |""".stripMargin

    val sql =
      s"""create table ${cache.getLocation}_2 as
         | ${sql12}
         |""".stripMargin
    loader.runSQL(sql)
    return true
  }

  private def calcPlanTable(): Boolean = {

    var orList: List[String] = List.apply()
    var tgtColumns: List[String] = List.apply()

    columnsWithChanges.foreach(i => {
      tgtColumns = tgtColumns.appended(s"       coalesce(tgt.${i}, src.${i}) as ${i},")
      orList = orList.appended(s"   or src.${i} <> tgt.${i}")
    })

    columns.foreach(i => {
      if i.isNullable then orList = orList.appended(s"   or (tgt.${i.columnName} is not null and src.${i.columnName} is null)")
    })

    columnWithOutChanges.foreach(i => {
      tgtColumns = tgtColumns.appended(s"       src.${i} as $i,")
    })

    uniqueKey.foreach(i => {
      tgtColumns = tgtColumns.appended(s"       tgt.${i} as $i,")
    })

    val sql: String =
      s"""create table ${cache.getLocation}_3 as
         |with src as (select * from ${targetTable} where valid_to_dttm = to_date('2100','yyyy') ${getExtraFilter}),
         |     tgt as (select * from ${cache.getLocation}_2)
         |select case when src.${uniqueKey.head} is not null then 'Update' else 'Insert' end as update_command,
         |       ${tgtColumns.mkString("\n")}
         |       now() as new_date
         |  from tgt
         |  left join src on ${getJoinString}
         |where src.${uniqueKey.head} is null
         |${orList.mkString("\n")}
         |""".stripMargin
    loader.runSQL(sql)
  }

  private def updateValidInterval(): Boolean = {
    val sql: String =
      s"""
         |update ${targetTable} tgt
         |   set valid_to_dttm = new_date - interval '1 microsecond'
         |  from ${cache.getLocation}_3 src
         | where tgt.valid_to_dttm = to_date('2100','yyyy')
         |   and src.update_command = 'Update'
         |   and ${getJoinString}""".stripMargin
    loader.runSQL(sql)
  }

  private def insertNewInterval(): Boolean = {
    val sql: String =
      s"""
         |insert into $targetTable(${(uniqueKey ::: columnsWithChanges ::: columnWithOutChanges).mkString(", ")},valid_from_dttm,valid_to_dttm)
         |select ${(uniqueKey ::: columnsWithChanges ::: columnWithOutChanges).mkString(", ")}, new_date as valid_from_dttm,to_date('2100','yyyy') as valid_to_dttm
         |  from ${cache.getLocation}_3
         |""".stripMargin
    loader.runSQL(sql)
  }

  private def deleteTemp(): Boolean = {
    loader.runSQL(s"""drop table ${cache.getLocation}_3""")
    loader.runSQL(s"""drop table ${cache.getLocation}_2""")
    loader.runSQL(s"""drop table ${cache.getLocation}""")
  }

  private def writeTargetMerge(df: DataFrame, list: List[Column]): Boolean = {
    val newDf: DataFrame = df.select(list *)
    LogMode.debugDF(newDf)
    cache.saveTable(DataFrameOriginal(newDf), WriteMode.overwrite)
    calDeltaTable()
    calcPlanTable()
    updateValidInterval()
    insertNewInterval()
    deleteTemp()

    return true
  }

  private def writeTargetMerge(): Boolean = {
    if uniqueKey.isEmpty then throw Exception()

    val df: DataFrameTrait = getSourceDf
    var list: List[Column] = List.apply()
    (uniqueKey ::: columnsWithChanges).foreach(i => list = list.appended(col(i)))
    df match
      case _ => writeTargetMerge(df.getDataFrame, list)
    return true
  }

  private def writeTargetBasic(): Boolean = {
    val df: DataFrameTrait = getSourceDf
    if uniqueKey.nonEmpty then throw Exception()
    df match
      case x: DataFramePartition => {
        if loadMode == WriteMode.overwrite then
          loader.truncateTable(targetTable)
        x.getPartitions.foreach(i => {
          loader.writeDf(i._2.getDataFrame, targetTable, WriteMode.append)
        })
      }
      case _ => loader.writeDf(df.getDataFrame, targetTable, loadMode)
    return true
  }

  override def writeTarget(): Boolean = {
    loadMode match
      case WriteMode.merge => writeTargetMerge()
      case WriteMode.append => writeTargetBasic()
      case WriteMode.overwrite => writeTargetBasic()
      case _ => throw Exception()
  }

  override def getSourceDf: DataFrameTrait = {
    if deduplicationKey.isEmpty then return super.getSourceDf
    val sql =
      s"""with a as (
         |  select *, row_number() over (partition by ${uniqueKey.mkString(",")} order by ${deduplicationKey.mkString(",")}) as rn
         |    from $source)
         |select *
         |  from a
         | where rn = 1""".stripMargin
    return DataFrameOriginal(SparkObject.spark.sql(sql))
  }
}