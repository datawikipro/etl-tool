package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.functions.lit
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

  def writeAutoPartition(df: DataFrameTrait, mode: WriteMode): Boolean = {
    if partitionBy.isEmpty then throw IllegalArgumentException("partitionBy cannot be empty for auto partition mode")

    loader.writeDfPartitionAuto(df.getDataFrame, tableName, targetFile, partitionBy, mode)
    return true

  }

  def writeDirectPartition(df: DataFrameTrait): Boolean = {
    loader.writeDfPartitionDirect(df.getDataFrame, tableName, targetFile, partitionBy, ApplicationContext.getPartitions(partitionBy *), loadMode, false)
    return true
  }

  def writeStream(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, tableName, targetFile, loadMode)
    return true
  }


  def writeStreamByRunId(df: DataFrameTrait): Boolean = {
    val listPartition: List[String] = List.apply("run_id")

    var dfWithExtraColumn = df.getDataFrame
    ApplicationContext.getPartitions2(listPartition *).foreach(col => {
      dfWithExtraColumn = dfWithExtraColumn.withColumn(col._1, lit(col._2))
    })

    loader.writeDfPartitionAuto(dfWithExtraColumn, tableName, targetFile, listPartition, loadMode)

    return true
  }

  def writeFullTable(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, tableName, targetFile, loadMode)
    return true
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
        
        val targetRef = icebergLoader.fullRef(targetFile)
        icebergLoader.createSchemaIfNotExists(targetFile)
        if (!SparkObject.spark.catalog.tableExists(targetRef)) {
          logInfo(s"Target table $targetRef does not exist. Performing initial load instead of MERGE.")
          icebergLoader.writeDf(df.getDataFrame, tableName, targetFile, WriteMode.overwriteTable, partitionBy)
          return true
        }

        val lastSlashIdx = targetFile.lastIndexOf('/')
        val s3SchemaFolder = if (lastSlashIdx != -1) targetFile.substring(0, lastSlashIdx) else s"$schemaName.db"
        val tempTableLocation = s"$s3SchemaFolder/$tempTable"

        logInfo(s"Step A: Writing DataFrame to temp table $tempTableName in Spark catalog $catalog")
        icebergLoader.writeDf(df.getDataFrame, tempTableName, tempTableLocation, WriteMode.overwriteTable, partitionBy)
        
        val tableLocation = s"$warehouse/$s3SchemaFolder/$tempTable"
        
        icebergLoader.getTrinoLoader match {
          case Some(trinoRegistry) =>
            logInfo(s"Step B: Registering temp table $tempTableName in Trino")
            trinoRegistry.registerTable(catalog, schemaName, tempTable, tableLocation)
            
            try {
              logInfo(s"Step C: Executing MERGE in Trino")
              trinoRegistry.executeMerge(catalog, schemaName, targetTable, tempTable, mergeKeys, df.getDataFrame.columns.toList)
            } finally {
              logInfo(s"Step D: Dropping temp table $tempTableName in Trino")
              trinoRegistry.dropTable(catalog, schemaName, tempTable)
              
              logInfo(s"Step E: Dropping temp table $tempTableName in Spark")
              try {
                SparkObject.spark.sql(s"DROP TABLE IF EXISTS $catalog.`$s3SchemaFolder`.$tempTable")
              } catch {
                case e: Exception =>
                  logWarning(s"Failed to drop temp table $tempTable in Spark database $s3SchemaFolder: ${e.getMessage}")
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