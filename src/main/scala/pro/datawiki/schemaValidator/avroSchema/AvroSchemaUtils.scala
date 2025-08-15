package pro.datawiki.schemaValidator.avroSchema

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaTemplate

/**
 * Утилиты для работы с Avro Schema и преобразования между различными форматами.
 */
object AvroSchemaUtils {

  /**
   * Преобразует BaseSchemaTemplate в строку Avro Schema.
   *
   * @param schemaTemplate Шаблон схемы для преобразования
   * @param namespace      Пространство имен для схемы Avro
   * @param recordName     Имя записи для схемы Avro
   * @param docString      Документация для схемы
   * @return Строка, представляющая Avro Schema
   */
  def convertTemplateToAvroSchema(schemaTemplate: BaseSchemaTemplate, 
                                 namespace: String = "com.datawiki", 
                                 recordName: String = "GeneratedRecord", 
                                 docString: String = "Schema generated from BaseSchemaTemplate"): String = {
    try {
      AvroSchemaWriter.convertToAvroSchema(schemaTemplate, namespace, recordName, docString)
    } catch {
      case e: Exception =>
        throw SchemaValidationException(s"Ошибка при преобразовании шаблона в Avro Schema: ${e.getMessage}", e)
    }
  }

}
