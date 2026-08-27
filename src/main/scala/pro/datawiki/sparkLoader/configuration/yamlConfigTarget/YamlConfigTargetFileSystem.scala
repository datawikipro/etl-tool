package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, lit, struct, transform}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.connection.minIo.minioIceberg.LoaderMinIoIceberg
import pro.datawiki.sparkLoader.SparkObject

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetFileSystem(
                                       connection: String,
                                       source: String,
                                       tableName:String,
                                       mode: String = "append",
                                       targetFile: String,
                                       partitionBy: List[String] = List.apply(),
                                       mergeKeys: List[String] = List.apply(),
                                     ) extends YamlConfigTargetBase(connection = connection, mode = mode, source = source) with YamlConfigTargetTrait with LoggingTrait {

  @JsonIgnore
  override def loader: FileStorageTrait = {
    super.loader match
      case x: FileStorageTrait => x
      case _ => throw IllegalArgumentException("Invalid loader type")
  }

  @JsonIgnore
  val effectiveTargetFile: String = Option(targetFile).filter(_.nonEmpty).getOrElse(tableName)

  def writeAutoPartition(df: DataFrameTrait, mode: WriteMode): Boolean = {
    if partitionBy.isEmpty then throw IllegalArgumentException("partitionBy cannot be empty for auto partition mode")

    loader.writeDfPartitionAuto(df.getDataFrame, tableName, effectiveTargetFile, partitionBy, mode)
    return true

  }

  def writeDirectPartition(df: DataFrameTrait): Boolean = {
    loader.writeDfPartitionDirect(df.getDataFrame, tableName, effectiveTargetFile, partitionBy, ApplicationContext.getPartitions(partitionBy *), loadMode, false)
    return true
  }

  def writeStream(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, tableName, effectiveTargetFile, loadMode)
    return true
  }


  def writeStreamByRunId(df: DataFrameTrait): Boolean = {
    val listPartition: List[String] = List.apply("run_id")

    var dfWithExtraColumn = df.getDataFrame
    ApplicationContext.getPartitions2(listPartition *).foreach(col => {
      dfWithExtraColumn = dfWithExtraColumn.withColumn(col._1, lit(col._2))
    })

    loader.writeDfPartitionAuto(dfWithExtraColumn, tableName, effectiveTargetFile, listPartition, loadMode)

    return true
  }

  def writeFullTable(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, tableName, effectiveTargetFile, loadMode)
    return true
  }

  private def alignToTargetSchema(df: DataFrame, targetSchema: StructType): DataFrame = {
    def alignStruct(parentCol: Column, sourceStruct: StructType, targetStruct: StructType): Column = {
      val sourceFieldMap = sourceStruct.fields.map(f => f.name.toLowerCase -> f).toMap
      val targetFields = targetStruct.fields.map { targetField =>
        sourceFieldMap.get(targetField.name.toLowerCase) match {
          case Some(srcField) =>
            val subCol = parentCol.getField(srcField.name)
            alignDataType(subCol, srcField.dataType, targetField.dataType).as(targetField.name)
          case None =>
            lit(null).cast(targetField.dataType).as(targetField.name)
        }
      }
      struct(targetFields*)
    }

    def alignDataType(colExpr: Column, srcType: DataType, targetType: DataType): Column = {
      (srcType, targetType) match {
        case (srcStruct: StructType, tgtStruct: StructType) =>
          alignStruct(colExpr, srcStruct, tgtStruct)
        case (ArrayType(srcElem: StructType, _), ArrayType(tgtElem: StructType, _)) =>
          transform(colExpr, (x: Column) => alignStruct(x, srcElem, tgtElem))
        case _ =>
          colExpr
      }
    }

    try {
      val targetFieldMap = targetSchema.fields.map(f => f.name.toLowerCase -> f).toMap
      val projectedCols = df.schema.fields.map { srcField =>
        targetFieldMap.get(srcField.name.toLowerCase) match {
          case Some(tgtField) =>
            val colExpr = col(s"`${srcField.name}`")
            alignDataType(colExpr, srcField.dataType, tgtField.dataType).as(srcField.name)
          case None =>
            col(s"`${srcField.name}`")
        }
      }
      df.select(projectedCols*)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to automatically align DataFrame struct schema to target table: ${e.getMessage}. Proceeding with original DataFrame.")
        df
    }
  }

  def writeMerge(df: DataFrameTrait): Boolean = {
    if (mergeKeys.isEmpty) throw IllegalArgumentException("mergeKeys cannot be empty for merge mode")
    
    val lastDotIdx = tableName.lastIndexOf('.')
    if (lastDotIdx == -1) throw IllegalArgumentException(s"tableName must be in format schema.table, got: $tableName")
    val schemaName = tableName.substring(0, lastDotIdx)
    val targetTable = tableName.substring(lastDotIdx + 1)
    val tempTable = s"${targetTable}_tmp_${System.currentTimeMillis()}"
    val tempTableName = s"$schemaName.$tempTable"
    
    logInfo(s"Starting merge write for $tableName with temp table $tempTableName")

    loader match {
      case icebergLoader: LoaderMinIoIceberg =>
        val catalog = icebergLoader.configYaml.catalog
        val warehouse = icebergLoader.configYaml.warehouse
        
        val targetRef = icebergLoader.fullRef(effectiveTargetFile)
        icebergLoader.createSchemaIfNotExists(effectiveTargetFile)
        if (!SparkObject.spark.catalog.tableExists(targetRef)) {
          logInfo(s"Target table $targetRef does not exist. Performing initial load instead of MERGE.")
          icebergLoader.writeDf(df.getDataFrame, tableName, effectiveTargetFile, WriteMode.overwriteTable, partitionBy)
          return true
        }

        // 1. Trino Schema Evolution & Column discovery
        val trinoTargetCols: Option[Set[String]] = icebergLoader.getTrinoLoader.map { trino =>
          val syncedColsMap = trino.syncTargetSchema(catalog, schemaName, targetTable, df.getDataFrame.schema)
          if (syncedColsMap.nonEmpty) syncedColsMap.keySet else Set.empty[String]
        }.filter(_.nonEmpty)

        // 2. Spark Schema Evolution & Target Schema Struct Alignment
        val alignedDf = try {
          val targetSchema = SparkObject.spark.table(targetRef).schema
          val targetColNames = targetSchema.fieldNames.toSet
          val missingFields = df.getDataFrame.schema.fields.filterNot(f => targetColNames.contains(f.name))
          
          if (missingFields.nonEmpty) {
            logInfo(s"Target table $targetRef is missing ${missingFields.length} columns in Spark: ${missingFields.map(_.name).mkString(", ")}. Adding them via ALTER TABLE.")
            val alterColsDef = missingFields.map(f => s"`${f.name}` ${f.dataType.sql}").mkString(", ")
            SparkObject.spark.sql(s"ALTER TABLE $targetRef ADD COLUMNS ($alterColsDef)")
            logInfo(s"Successfully evolved schema in Spark for $targetRef")
          }
          
          val updatedTargetSchema = SparkObject.spark.table(targetRef).schema
          alignToTargetSchema(df.getDataFrame, updatedTargetSchema)
        } catch {
          case e: Exception =>
            logWarning(s"Could not evolve/align schema in Spark for $targetRef: ${e.getMessage}")
            df.getDataFrame
        }

        val (locSchemaName, _) = icebergLoader.parseLocation(effectiveTargetFile)
        val s3SchemaFolder = if (locSchemaName.endsWith(".db")) locSchemaName else locSchemaName + ".db"
        val tempTableLocation = s"$s3SchemaFolder/$tempTable"

        logInfo(s"Step A: Writing DataFrame to temp table $tempTableName in Spark catalog $catalog")
        icebergLoader.writeDf(alignedDf, tempTableName, tempTableLocation, WriteMode.overwriteTable, partitionBy)
        
        icebergLoader.getTrinoLoader match {
          case Some(trinoRegistry) =>
            try {
              logInfo(s"Step B: Executing MERGE in Trino")
              trinoRegistry.executeMerge(catalog, schemaName, targetTable, tempTable, mergeKeys, alignedDf.columns.toList, trinoTargetCols)
            } finally {
              logInfo(s"Step C: Dropping temp table $tempTableName in Trino")
              trinoRegistry.dropTable(catalog, schemaName, tempTable)
              
              logInfo(s"Step D: Dropping temp table $tempTableName in Spark")
              try {
                val tempRef = icebergLoader.fullRef(tempTableLocation)
                SparkObject.spark.sql(s"DROP TABLE IF EXISTS $tempRef")
              } catch {
                case e: Exception =>
                  logWarning(s"Failed to drop temp table in Spark: ${e.getMessage}")
              }
            }
            
          case _ =>
            throw IllegalArgumentException("Trino JDBC registry is required for merge mode")
        }
        
      case _ =>
        throw UnsupportedOperationException("Merge mode is only supported for minioIceberg loader")
    }
    
    return true
  }

  @JsonIgnore
  override def writeTarget(): Boolean = {
    val df: DataFrameTrait = getSourceDf

    WriteMode(mode) match {
      case WriteMode.autoOverwrite => writeAutoPartition(df, WriteMode.overwriteTable)
      case WriteMode.autoAppend => writeAutoPartition(df, WriteMode.append)
      //      case WriteMode.direct => writeDirectPartition(df)
      case WriteMode.stream => writeStream(df)
      case WriteMode.streamByRunId => writeStreamByRunId(df)
      case WriteMode.overwriteTable => writeDirectPartition(df)
      case WriteMode.overwritePartition => writeDirectPartition(df)
      case WriteMode.mergeDelta => writeMerge(df)
      case WriteMode.mergeFull => writeMerge(df)
      //      case WriteMode.none => writeFullTable(df)

      case fs => {
        throw UnsupportedOperationException(s"Unsupported partition mode: $fs")
      }
    }
  }
}