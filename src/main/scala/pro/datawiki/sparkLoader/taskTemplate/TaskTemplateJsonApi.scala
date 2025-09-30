package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameEmpty, DataFrameError, DataFrameTrait}
import pro.datawiki.exception.{DataProcessingException, NotImplementedException}
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObjectTemplate, BaseSchemaTemplate}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceWeb.YamlConfigSchema
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.context.ApplicationContext


class TaskTemplateJsonApi(source: LoaderJsonApi, potentialSchemasList: List[YamlConfigSchema]) extends TaskTemplate {

  val migrationJson: Migration = Migration("json")
  val migrationProjectSchema: Migration = Migration("projectSchema")
  
  private def getDataFrameFromJsonNonSchema(json:String):DataFrameTrait={
    try {
      val extractedSchema: BaseSchemaTemplate = migrationJson.extractSchema(json)
      extractedSchema match {
        case x: BaseSchemaObjectTemplate => if x.inElements.isEmpty then return DataFrameEmpty()
        case _ =>
      }
      return migrationJson.getDataFrameFromString(
        json,
        null,
        extractedSchema,
        true,
        isStrongValidation = true)
    } catch {
      case e: Exception => {
        throw new DataProcessingException(s"Error processing JSON (no schema): ${json.take(500)}${if (json.length > 500) "..." else ""}. Original error: ${e.getMessage}", e)
      }
    }
  }

  private def getDataFrameFromJsonWithSchema(json:String): DataFrameTrait = {
    try {
      val result: DataFrameTrait = migrationJson.getDataFrameFromString(
        inString = json,
        schemaName = null,
        inSchema = migrationProjectSchema.readTemplate(potentialSchemasList.head.fileLocation),
        validData = true,
        isStrongValidation = true
      )
      result match {
        case fs => return fs
      }
    } catch {
      case e: Exception => {
        throw new DataProcessingException(s"Error processing JSON (single schema): ${json.take(500)}${if (json.length > 500) "..." else ""}. Original error: ${e.getMessage}", e)
      }
    }
  }

  private def getDataFrameFromJsonMultySchema(json:String): DataFrameTrait = {
    potentialSchemasList.foreach(
      i => {
        val baseSchema: BaseSchemaTemplate = migrationProjectSchema.readTemplate(i.fileLocation)
        val result = try {
           migrationJson.getDataFrameFromString(
            inString = json,
            schemaName = i.schemaName,
            inSchema = baseSchema,
            validData = i.isError,
            isStrongValidation = false)
          
        } catch {
          case _: Throwable => new DataFrameError()
        }
        result match {
          case x: DataFrameError => {}
          case fs =>
            return fs
        }})

      throw DataProcessingException(
        s"""No valid schema found for JSON
           |data json:
           |$json""".stripMargin)

  }
  
  private def getDataFrameFromJson(json: String): DataFrameTrait = {
      if (json == null || json.isEmpty) {
        throw DataProcessingException("Пустая или null JSON строка передана в getDataFrameFromJson")
      }
      potentialSchemasList.length match {
        case 0 => return getDataFrameFromJsonNonSchema(json)
        case 1 => return getDataFrameFromJsonWithSchema(json)
        case _ => return getDataFrameFromJsonMultySchema(json)
      }
    }

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val variablesList: Map[String, String] = source.getVariables(parameters)

    val resTxt = source.sendRequest(variablesList)
    
    if resTxt == null then return List.empty
    val df: DataFrameTrait = source.in.resultType match {
      case "json" => getDataFrameFromJson(resTxt)

      case _ => {
        val lines: Array[String] = resTxt.split("\n")
        //      val ds: Dataset[String] = SparkObject.spark.createDataset(lines)
        //       Читаем RDD как DataFrame
        //      val df:DataFrame = SparkObject.spark.read
        //        .option("header", "true") // используем первую строку как заголовок
        //        .option("delimiter", ";") // задаём разделитель
        //        .option("inferSchema", "true") // автоматическое определение типов данных
        //        .csv(ds)
        //
        //      df.show()
        //      df.printSchema()
        throw NotImplementedException("Non-JSON result types not implemented")
      }
    }
    df match {
      case x: DataFrameEmpty => return List.apply(x)
      case _ =>
    }

    if parameters != null then {
      parameters.foreach(j => df.addConstantColumn(j._1, variablesList(s"$${${j._1}}")))
    }
    
    ApplicationContext.getPartitions2("run_id").foreach(col=> df.addConstantColumn(col._1, col._2))
    
    LogMode.debugDF(df.getDataFrame)

    return List.apply(df)
  }
}
