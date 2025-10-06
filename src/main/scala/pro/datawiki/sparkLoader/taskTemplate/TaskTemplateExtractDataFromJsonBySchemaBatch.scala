package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.{DataProcessingException, NotImplementedException}
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.schemaValidator.spark.SparkConstructor
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElementRow
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressMode
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable


class TaskTemplateExtractDataFromJsonBySchemaBatch(tableName: String,
                                                   jsonColumn: String,
                                                   jsonResultColumn: String,
                                                   baseSchema: String,
                                                   mergeSchema: Boolean,
                                                   loadMode: ProgressMode
                                                  ) extends TaskTemplate with LoggingTrait {

  val migrationJson: Migration = Migration("json")
  val migrationProjectSchema: Migration = Migration("projectSchema")

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("extract schema", s"table: $tableName, column: $jsonColumn")

    try {
      var df: DataFrame = SparkObject.spark.sql(s"""select * from $tableName""")
      LogMode.debugDF(df)

      val savedSchema: BaseSchemaTemplate = migrationProjectSchema.readTemplate(baseSchema)
      var masterSchema: BaseSchemaTemplate = savedSchema

      if mergeSchema then {
        val list = df/*.filter(s"$jsonColumn <> '' and $jsonColumn is not null")*/.select(s"$jsonColumn").distinct().collect().toList
        logInfo(s"Found ${list.length} distinct JSON values in column: $jsonColumn")
        val jsons: List[String] = list.map(i => i.get(0).toString)

        // Извлекаем схему из JSON данных один раз
        logInfo("Extracting schema from JSON data")
        masterSchema = migrationJson.evolutionSchemaByList(jsons, masterSchema)
        migrationProjectSchema.writeTemplate(baseSchema, masterSchema) match {
          case true =>
          case false => throw DataProcessingException("Failed to write schema template")
        }
      }
      
      df = loadMode match {
        case ProgressMode.batch => df.withColumn(jsonResultColumn, from_json(col(jsonColumn), masterSchema.getSparkRowElementTemplate.getType))
        case ProgressMode.row => {

          var baseSchemaTemplate: BaseSchemaObjectTemplate = SparkConstructor.getBaseSchemaTemplate(df.schema)
          baseSchemaTemplate = baseSchemaTemplate.appendBaseSchemaTemplate(jsonResultColumn, masterSchema)
          val rdd: List[BaseSchemaObject] = df.rdd.collect().toList.map(row => SparkRowElementRow(row, df.schema).getBaseSchemaObject)

          val newRdd = rdd.map(col => {
            try {
              val a: BaseSchemaStruct = col.getElementStructByName(jsonColumn,null)
              val a1 = a match {
                case x: BaseSchemaString => x.getValue
                case _ => {
                  throw DataProcessingException("Expected BaseSchemaString for JSON column extraction")
                }
              }
              val b = migrationJson.extractStruct(a1)
              val c = masterSchema.extractDataFromObject(b)
              col.appendElement(jsonResultColumn, c)
            } catch {
              case e: Exception => {
                throw e
              }
            }
          }
          )
          df = SparkConstructor.getDataFrameFromListBaseSchemaObjects(newRdd, baseSchemaTemplate)
          LogMode.debugDF(df)
          return List.apply(DataFrameOriginal(df))
        }
        case _ => {
          throw NotImplementedException("Unsupported load mode for JSON extraction")
        }
      }

      LogMode.debugDF(df)
      logOperationEnd("extract schema", startTime, s"table: $tableName")
      return List.apply(DataFrameOriginal(df))
    } catch {
      case e: Exception =>
        logError("extract schema", e, s"table: $tableName")
        throw e
    }
  }

}
