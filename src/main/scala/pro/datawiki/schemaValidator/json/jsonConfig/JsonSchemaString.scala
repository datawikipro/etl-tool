package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JString
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaString, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет строковое значение в JSON схеме.
 *
 * @param value Строковое значение
 */
case class JsonSchemaString(value: String) extends JsonStruct {
  /**
   * Преобразует строковое значение в структуру базовой схемы.
   *
   * @return Структура базовой схемы для строкового значения
   */
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaString(value, false)

  override def isEmpty: Boolean = false
}


object JsonSchemaString {
  /**
   * Создает JsonSchemaString из JSON строкового значения.
   *
   * @param jString JSON строковое значение
   * @return Представление JSON строки в виде JsonSchemaString
   */
  def apply(jString: JString): JsonSchemaString = JsonSchemaString(jString.s)
}
