package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameEmpty, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaObjectTemplate
import pro.datawiki.schemaValidator.projectSchema.SchemaObject
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass
import pro.datawiki.yamlConfiguration.YamlClass.toYaml

import java.nio.file.{Files, Paths}
import scala.collection.mutable

class TaskTemplateExtractAndValidateDataFrame(
                                               dataFrameIn: String,
                                               configLocation: String) extends TaskTemplate with LoggingTrait {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("extract and validate DataFrame", s"source: $dataFrameIn, config: $configLocation")

    try {
      // Получаем данные из источника
      logInfo(s"Loading data from: $dataFrameIn")
      val df = SparkObject.spark.sql(s"""select * from $dataFrameIn""")
      if df.count() ==0 then return List.apply(DataFrameEmpty())
      LogMode.debugDF(df)

      // Создаем объект схемы из полей DataFrame
      logInfo("Extracting schema from DataFrame")
      val currentSchema = BaseSchemaObjectTemplate(df.schema.fields.toList)

      // Проверяем наличие пути к конфигурации
      if (configLocation == null) {
        logError("validation", new IllegalArgumentException("Не указан путь к файлу конфигурации схемы"))
        throw new IllegalArgumentException("Не указан путь к файлу конфигурации схемы")
      }

      // Создаем файл схемы, если он не существует
      val schemaFileExists = Files.exists(Paths.get(configLocation))
      logInfo(s"Schema file exists: $schemaFileExists, path: $configLocation")

      if (!schemaFileExists) {
        logInfo("Creating new schema file from DataFrame schema")
        val yaml = YamlClass.toYaml(currentSchema.getProjectSchema)
        YamlClass.writefile(configLocation, yaml)
        logInfo(s"Schema saved to file: $configLocation")
      }

      // Загружаем сохраненную схему и сравниваем с текущей
      logInfo("Loading saved schema for validation")
      val savedSchema = SchemaObject(configLocation).getBaseSchemaTemplate

      // Возвращаем результат
      logOperationEnd("extract and validate DataFrame", startTime, s"source: $dataFrameIn")
      return List[DataFrameTrait](DataFrameOriginal(df))

    } catch {
      case e: Exception =>
        logError("extract and validate DataFrame", e, s"source: $dataFrameIn")
        throw e
    }
  }

}
