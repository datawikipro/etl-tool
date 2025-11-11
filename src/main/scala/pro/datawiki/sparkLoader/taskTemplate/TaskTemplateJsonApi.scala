package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.Dataset
import pro.datawiki.datawarehouse.{DataFrameEmpty, DataFrameError, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.{DataProcessingException, NotImplementedException}
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObjectTemplate, BaseSchemaTemplate}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceWeb.YamlConfigSchema
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

class TaskTemplateJsonApi(
                           source: LoaderJsonApi,
                           potentialSchemasList: List[YamlConfigSchema],
                           validateStatusColumn: String,
                           validateStatusValue: List[String]
                         ) extends TaskTemplate {

  val migrationJson: Migration = Migration("json")
  val migrationProjectSchema: Migration = Migration("projectSchema")

  private def getDataFrameFromJsonNonSchema(json: String): DataFrameTrait = {
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

  private def getDataFrameFromJsonWithSchema(json: String): DataFrameTrait = {
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

  private def getDataFrameFromJsonMultySchema(json: String): DataFrameTrait = {
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
        }
      })

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

  private def getDataFrameFromCsv(csv: String): DataFrameTrait = {
    if (csv == null || csv.isEmpty) {
      throw DataProcessingException("Пустая или null JSON строка передана в getDataFrameFromJson")
    }
    val spark = SparkObject.spark
    import spark.implicits.*
    val lines: Seq[String] = csv.split("\\n").toSeq
    val linesDS: Dataset[String] = lines.toDS()
    val df = spark.read.
      option("header", "true") // Первая строка - это заголовок
      .option("delimiter", "\t") // Разделитель - табуляция
      .option("inferSchema", "true") // Попросить Spark угадать типы данных
      .csv(linesDS)
    LogMode.debugDF(df)
    return DataFrameOriginal(df)

  }

  private def getResult(variablesList: Map[String, String]): DataFrameTrait = {
    val resTxt = source.sendRequest(variablesList)

    if resTxt == null then return DataFrameEmpty()
    val df: DataFrameTrait = source.in.resultType match {
      case "json" => getDataFrameFromJson(resTxt)
      case "csv" => getDataFrameFromCsv(resTxt)
      case _ => {
        val lines: Array[String] = resTxt.split("\n")
        throw NotImplementedException("Non-JSON result types not implemented")
      }
    }
    df match {
      case x: DataFrameEmpty => return x
      case fx => return fx
    }
  }

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val variablesList: Map[String, String] = source.getVariables(parameters)
    var df: DataFrameTrait = null
    if validateStatusColumn != null then {
      var status = "undefined"
      while (!validateStatusValue.contains(status)) {

        df = getResult(variablesList)
        LogMode.debugDF(df.getDataFrame)
        try {
          val a = df.getDataFrame.select(validateStatusColumn).collect()
          val firstRow = a.head
          status = firstRow.get(0).toString
          if !validateStatusValue.contains(status) then {
            println(s"current status $status")
            Thread.sleep(1000)
          }

        } catch {
          case e => {
            throw e
          }
        }


      }
    } else {
      df = getResult(variablesList)
    }

    if parameters != null then {
      parameters.foreach(j => {
        try {
          df.addConstantColumn(j._1, variablesList(s"$${${j._1}}"))
        }
        catch {
          case e => {
            throw e
          }
        }
      })
    }

    ApplicationContext.getPartitions2("run_id").foreach(col => df.addConstantColumn(col._1, col._2))

    LogMode.debugDF(df.getDataFrame)

    return List.apply(df)
  }
}
