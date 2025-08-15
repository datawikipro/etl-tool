package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.SchemaValidator
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaObjectTemplate
import pro.datawiki.schemaValidator.projectSchema.SchemaObject
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass
import pro.datawiki.yamlConfiguration.YamlClass.toYaml

import java.nio.file.{Files, Paths}
import scala.collection.mutable

class TaskTemplateExtractAndValidateDataFrame(
                                               dataFrameIn: String,
                                               configLocation: String) extends TaskTemplate {
  /**
   * Запускает задачу извлечения и валидации DataFrame.
   * Задача выполняет следующие шаги:
   * 1. Получает данные из SQL-запроса
   * 2. Извлекает схему из данных
   * 3. Сохраняет схему в файл, если она не существует
   * 4. Проверяет соответствие схемы данных сохраненной схеме
   *
   * @param parameters Параметры выполнения задачи
   * @param isSync Флаг синхронного выполнения
   * @return Список объектов DataFrame
   * @throws Exception при ошибках выполнения
   */
  override def run(parameters: mutable.Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    // Получаем данные из источника
    val df = SparkObject.spark.sql(s"""select * from $dataFrameIn""")
    LogMode.debugDF(df)

    // Создаем объект схемы из полей DataFrame
    val currentSchema = BaseSchemaObjectTemplate(df.schema.fields.toList)

    // Проверяем наличие пути к конфигурации
    if (configLocation == null) {
      throw new IllegalArgumentException("Не указан путь к файлу конфигурации схемы")
    }

    // Создаем файл схемы, если он не существует
    if (!Files.exists(Paths.get(configLocation))) {
      val yaml = YamlClass.toYaml(currentSchema.getProjectSchema)
      YamlClass.writefile(configLocation, yaml)
      println(s"Схема сохранена в файл: $configLocation")
    }

    // Загружаем сохраненную схему
    val savedSchema = SchemaObject(configLocation).getBaseSchemaTemplate

    // Проверяем соответствие схем
    if (!SchemaValidator.validateDiffSchemas(savedSchema, currentSchema)) {
      throw new SchemaValidationException(
        s"Схема данных не соответствует сохраненной схеме в $configLocation"
      )
    }

    // Возвращаем результат
    List[DataFrameTrait](DataFrameOriginal(df))
  }

}
