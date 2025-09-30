package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFramePartition, DataFrameTrait}
import pro.datawiki.exception.{DataProcessingException, IllegalArgumentException}
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.yamlConfigTargetDatabase.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType.{Text, Varchar}
import pro.datawiki.sparkLoader.connection.spark.LoaderSpark
import pro.datawiki.sparkLoader.dictionaryEnum.{SCDType, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetDatabase(
                                     connection: String,
                                     source: String,
                                     mode: String = "append",
                                     targetSchema: String = throw IllegalArgumentException("targetSchema is required"),
                                     targetTable: String = throw IllegalArgumentException("targetTable is required"),
                                     columns: List[YamlConfigTargetColumn],
                                     uniqueKey: List[String] = List.apply(),
                                     deduplicationKey: List[String] = List.apply(),
                                     afterDQ: List[String] = List.apply(),
                                     partitionBy: List[String] = List.apply(),
                                   ) extends YamlConfigTargetBase(connection = connection, mode = mode, source = source) with YamlConfigTargetTrait with LoggingTrait {
  private var locCache: TransformationCacheDatabase = null
  @JsonIgnore
  def cache: TransformationCacheDatabase = {
    if locCache == null then locCache = new TransformationCacheDatabase()
    return locCache
  }

  @JsonIgnore
  override def loader: DatabaseTrait = {
    super.loader match
      case x: DatabaseTrait => x
      case _ =>
        throw IllegalArgumentException("Loader is not a DatabaseTrait")
  }

  private var locAllFields: List[String] = List.empty

  private def targetColumns: List[String] = {
    if locAllFields.nonEmpty then return locAllFields

    val dfTarget = loader.readDfSchema(targetSchema, targetTable)
    locAllFields = dfTarget.schema.fields.map(i => i.name).toList

    return locAllFields
  }

  private var locFieldsWithChanges: List[YamlConfigTargetColumn] = List.empty

  private def columnsWithChanges: List[YamlConfigTargetColumn] = {
    if locFieldsWithChanges.nonEmpty then return locFieldsWithChanges
    return columns.filter(col => !uniqueKey.toSet.contains(col.columnName))
  }

  private var locFieldsWithoutChanges: List[String] = List.empty

  private def columnWithOutChanges: List[String] = {
    if locFieldsWithoutChanges.nonEmpty then return locFieldsWithoutChanges
    locFieldsWithoutChanges = targetColumns diff (uniqueKey ::: columnsWithChanges.map(col1 => col1.columnName) ::: List.apply("valid_from_dttm", "valid_to_dttm"))
    return locFieldsWithoutChanges
  }

  @JsonIgnore
  private def getJoinString: String = {
    return uniqueKey.map(i => {
      s"src.${i} = tgt.${i}"
    }).mkString(" and ")
  }

  private var extraFilter: String = ""

  private def getExtraFilter: String = {
    if extraFilter != "" then return extraFilter

    if partitionBy.nonEmpty then {
      val sql1 = s"""select min(${partitionBy.head}) as min_partition from ${cache.getLocation}"""
      val minPartition = loader.getDataFrameBySQL(sql1).collect().head.get(0).toString
      extraFilter = s"""and ${partitionBy.head} >= '${minPartition}'"""
    }

    return extraFilter
  }

  @JsonIgnore
  private def calDeltaTable(): Boolean = {
    val sql12: String =
      s"""select ${(uniqueKey ::: columnsWithChanges.map(col1 => col1.columnName)).mkString(",")} from ${cache.getLocation}
         |except
         |select ${(uniqueKey ::: columnsWithChanges.map(col1 => col1.columnName)).mkString(",")} from $targetSchema.${targetTable} where valid_to_dttm = to_date('2100','yyyy') ${getExtraFilter}
         |""".stripMargin

    val sql =
      s"""create table ${cache.getLocation}_2 as
         | ${sql12}
         |""".stripMargin
    try {
      loader.runSQL(sql)
      return true
    } catch {
      case _ => {
        println(sql)
        throw DataProcessingException("Failed to execute SQL query")
      }
    }

  }

  @JsonIgnore
  private def calcPlanTable(): Boolean = {

    val orList: List[String] = columns.map(col => {
      col.isNullable match {
        case true => (
          s"""   or (tgt.${col.columnName} is not null and src.${col.columnName} is null)
             |   or src.${col.columnName} <> tgt.${col.columnName}
             |""".stripMargin)
        case false => s"   or src.${col.columnName} <> tgt.${col.columnName}"
      }
    })

    val tgtColumns: List[String] = List.empty
      ::: columnsWithChanges.map(col1 => s"       coalesce(tgt.${col1.columnName}, src.${col1.columnName}) as ${col1.columnName},")
      ::: columnWithOutChanges.map(i => (s"       src.${i} as $i,"))
      ::: uniqueKey.map(i => (s"       tgt.${i} as $i,"))

    val sql: String =
      s"""create table ${cache.getLocation}_3 as
         |with src as (select * from $targetSchema.${targetTable} where valid_to_dttm = to_date('2100','yyyy') ${getExtraFilter}),
         |     tgt as (select * from ${cache.getLocation}_2)
         |select case when src.${uniqueKey.head} is not null then 'Update' else 'Insert' end as update_command,
         |       ${tgtColumns.mkString("\n")}
         |       now() as new_date
         |  from tgt
         |  left join src on ${getJoinString}
         |where src.${uniqueKey.head} is null
         |${orList.mkString("\n")}
         |""".stripMargin
    try {
      loader.runSQL(sql)
    } catch {
      case e: Exception => {
        println(sql)
        throw DataProcessingException("Failed to execute SQL query", e)
      }
    }
  }

  @JsonIgnore
  private def updateValidInterval(): Boolean = {
    val sql: String =
      s"""
         |update $targetSchema.${targetTable} tgt
         |   set valid_to_dttm = new_date - interval '1 microsecond'
         |  from ${cache.getLocation}_3 src
         | where tgt.valid_to_dttm = to_date('2100','yyyy')
         |   and src.update_command = 'Update'
         |   and ${getJoinString}""".stripMargin
    try {
      return loader.runSQL(sql)
    } catch {
      case _: Throwable => {
        println(sql)
        throw DataProcessingException("Failed to execute SQL query")
      }
    }
  }

  @JsonIgnore
  private def insertNewInterval(): Boolean = {
    val sql: String =
      s"""
         |insert into $targetSchema.$targetTable(${(uniqueKey ::: columnsWithChanges.map(col1 => col1.columnName) ::: columnWithOutChanges).mkString(", ")},valid_from_dttm,valid_to_dttm)
         |select ${(uniqueKey ::: columnsWithChanges.map(col1 => col1.columnName) ::: columnWithOutChanges).mkString(", ")}, new_date as valid_from_dttm,to_date('2100','yyyy') as valid_to_dttm
         |  from ${cache.getLocation}_3
         |""".stripMargin
    try {
      loader.runSQL(sql)
    } catch {
      case _ => {
        println(sql)
        throw DataProcessingException("Failed to execute SQL query")
      }
    }
  }

  @JsonIgnore
  private def deleteTemp(): Boolean = {
    loader.runSQL(s"""drop table if exists ${cache.getLocation}_3""")
    loader.runSQL(s"""drop table if exists ${cache.getLocation}_2""")
    loader.runSQL(s"""drop table if exists ${cache.getLocation}""")
  }

  @JsonIgnore
  private def writeTargetMerge(newDf: DataFrame): Boolean = {

    try {
      LogMode.debugDF(newDf)
      cache.saveTable(DataFrameOriginal(newDf), WriteMode.overwriteTable, loader)
      calDeltaTable()
      calcPlanTable()

      updateValidInterval()
      insertNewInterval()

    } catch {
      case e: Exception => {
        logError("merge operation", e, s"table: $targetTable")
        throw DataProcessingException("Failed to process data", e)
      }
    } finally {
      deleteTemp()
    }
    return true
  }

  private def dataFrameDecodeDoubleToString(df: DataFrame, columnName: String) = {
    df.withColumn(colName = columnName, col = col(columnName).cast(StringType))
  }

  private def dataFrameDecodeStringToLong(df: DataFrame, columnName: String) = {
    df.withColumn(colName = columnName, col = col(columnName).cast(LongType))
  }

  private def dataFrameDecodeLongToString(df: DataFrame, columnName: String) = {
    df.withColumn(colName = columnName, col = col(columnName).cast(StringType))
  }


  private def dataFrameDecodeLongToTimestamp(df: DataFrame, columnName: String) = {
    df.withColumn(columnName,
      when(col(columnName).isNull, null)
        .when(col(columnName) < 10000000000L, // 10^10, похоже на секунды
          col(columnName).cast(TimestampType)
        )
        .when(col(columnName) < 10000000000000L, // 10^13, похоже на миллисекунды
          (col(columnName) / 1000).cast(TimestampType)
        )
        .when(col(columnName) < 10000000000000000L, // 10^16, похоже на микросекунды
          (col(columnName) / 1000000).cast(TimestampType)
        )
        .otherwise(null) // Если число слишком большое, считаем его невалидным
    )

  }

  private def dataFrameColumnTypeDecode(df: DataFrame, columnName: String, columnType: String, isTypeDecode: Boolean): DataFrame = {
    val sourceType1 = df.schema.fields.filter(col => {
      col.name == columnName
    })
    var sourceType: String = ""
    try {
      sourceType = sourceType1.head.dataType.typeName
    } catch {
      case e: Exception => {
        throw e
      }
    }

    val targetType = loader.decodeDataType(columnType)

    val sourceTypeEmin = LoaderSpark.decodeDataType(sourceType)
    if sourceTypeEmin.getMasterType == targetType.getMasterType then return df
    if !isTypeDecode then
      throw DataProcessingException(s"Type conversion not allowed: column '$columnName' from $sourceTypeEmin to $targetType")

    sourceTypeEmin match {
      case TableMetadataType.Bigint => {
        targetType match {
          case TableMetadataType.TimestampWithoutTimeZone => return dataFrameDecodeLongToTimestamp(dataFrameDecodeStringToLong(df, columnName), columnName) //TODO
          case TableMetadataType.String => return dataFrameDecodeLongToString(df, columnName)
          case TableMetadataType.Varchar => return dataFrameDecodeLongToString(df, columnName)
          case TableMetadataType.Text => return dataFrameDecodeLongToString(df, columnName)
          case _ => {
            throw DataProcessingException(s"Unsupported type conversion: column '$columnName' from Bigint to $targetType")
          }
        }
      }
      case TableMetadataType.Integer => {
        targetType match {
          case TableMetadataType.TimestampWithoutTimeZone => return dataFrameDecodeLongToTimestamp(dataFrameDecodeStringToLong(df, columnName), columnName) //TODO
          case TableMetadataType.Varchar | TableMetadataType.Text | TableMetadataType.String => return dataFrameDecodeLongToString(df, columnName)
          case _ => {
            throw DataProcessingException(s"Unsupported type conversion: column '$columnName' from Bigint to $targetType")
          }
        }
      }
      case TableMetadataType.String => {
        targetType match {
          case TableMetadataType.TimestampWithoutTimeZone =>            return dataFrameDecodeLongToTimestamp(df, columnName)
          case TableMetadataType.Integer => return dataFrameDecodeStringToLong(df, columnName) //TO
          case TableMetadataType.Bigint => return dataFrameDecodeStringToLong(df, columnName)
          case TableMetadataType.Varchar => return df
          case TableMetadataType.Text => return df
          case _ => {
            throw DataProcessingException(s"Unsupported type conversion: column '$columnName' from $sourceTypeEmin to $targetType")
          }
        }
      }
      case TableMetadataType.DoublePrecision => {
        targetType match {
          case TableMetadataType.String => return dataFrameDecodeDoubleToString(df, columnName)
          case TableMetadataType.Text => return dataFrameDecodeDoubleToString(df, columnName)

          case _ => {
            throw DataProcessingException(s"Unsupported type conversion: column '$columnName' from $sourceTypeEmin to $targetType")
          }
        }
      }
      case _ => {
        throw DataProcessingException(s"Unsupported type conversion: column '$columnName' from $sourceTypeEmin to $targetType")
      }
    }


  }

  private def prepareDataFrame(df: DataFrame): DataFrame = {
    var locDf: DataFrame = df
    var list: List[Column] = (uniqueKey ::: columnsWithChanges.map(col1 => col1.columnName)).map(col1 => col(col1))

    columnsWithChanges.foreach(col => {
      locDf = dataFrameColumnTypeDecode(locDf, col.columnName, col.columnType, col.columnTypeDecode)
      LogMode.debugDF(locDf)
    })
    return locDf.select(list *)
  }

  @JsonIgnore
  override def writeTarget(): Boolean = {
    loadMode match
      case WriteMode.merge => {
        getSourceDf match {
          case x: DataFrameOriginal => {
            if uniqueKey.isEmpty then throw IllegalArgumentException("Unique key is required for merge operation")
            writeTargetMerge(prepareDataFrame(x.getDataFrame))
          }
          case _ => throw IllegalArgumentException("Unsupported write mode")
        }
      }
      case WriteMode.append => {
        getSourceDf match
          case x: DataFramePartition => {
            x.getPartitions.foreach(i => loader.writeDfAppend(prepareDataFrame(i._2.getDataFrame), targetSchema, targetTable, SCDType.SCD_2, columns.map(col=> col.columnName), uniqueKey))
            return true
          }
          case x: DataFrameOriginal => {
            loader.writeDfAppend(prepareDataFrame(x.getDataFrame), targetSchema, targetTable, SCDType.SCD_2,  columns.map(col=> col.columnName), uniqueKey)
            return true
          }
          case _ => throw IllegalArgumentException("Unsupported write mode")
      }
      case WriteMode.overwriteTable => {
        getSourceDf match
          case x: DataFramePartition => throw UnsupportedOperationException("Overwrite table is not supported for partitioned tables")
          case x: DataFrameOriginal => {
            loader.writeDfOverwriteTable(prepareDataFrame(x.getDataFrame), targetSchema, targetTable, SCDType.SCD_2,  columns.map(col=> col.columnName), uniqueKey)
            return true
          }
          case _ => throw IllegalArgumentException("Unsupported write mode")
      }
      case WriteMode.overwritePartition => {
        getSourceDf match
          case x: DataFramePartition => {
            x.getPartitions.foreach(i => loader.writeDfOverwritePartition(prepareDataFrame(i._2.getDataFrame), targetSchema, targetTable, SCDType.SCD_2,  columns.map(col=> col.columnName), uniqueKey))
            return true
          }
          case x: DataFrameOriginal => {
            loader.writeDfAppend(prepareDataFrame(x.getDataFrame), targetSchema, targetTable, SCDType.SCD_2,  columns.map(col=> col.columnName), uniqueKey)
            return true
          }
          case _ => throw IllegalArgumentException("Unsupported write mode")
      }
      case _ => throw IllegalArgumentException("Unsupported write mode")
  }

  @JsonIgnore
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