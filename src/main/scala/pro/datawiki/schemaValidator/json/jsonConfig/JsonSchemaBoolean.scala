package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JBool
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaBoolean, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет булево значение в JSON схеме.
 *
 * @param value Значение boolean.
 */
class JsonSchemaBoolean(value: Boolean) extends JsonStruct {
  /**
   * Преобразует булево значение в структуру базовой схемы.
   *
   * @return Структура базовой схемы для булева значения.
   */
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaBoolean(value, false)

  override def isEmpty: Boolean = false
}

object JsonSchemaBoolean {
  /**
   * Создает JsonSchemaBoolean из JSON булева значения.
   *
   * @param in JSON булево значение.
   * @return Представление JSON булева значения в виде JsonSchemaBoolean.
   */
  def apply(in: JBool): JsonSchemaBoolean = new JsonSchemaBoolean(in.value)
}
