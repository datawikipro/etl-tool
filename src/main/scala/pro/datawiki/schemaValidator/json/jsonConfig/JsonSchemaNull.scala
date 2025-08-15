package pro.datawiki.schemaValidator.json.jsonConfig

import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaNull, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет значение null в JSON схеме.
 */
case object JsonSchemaNull extends JsonStruct {
  /**
   * Преобразует null-значение в структуру базовой схемы.
   *
   * @return Структура базовой схемы для null-значения
   */
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaNull(false)
  /**
   * Метод для совместимости с предыдущей реализацией.
   * 
   * @return Экземпляр JsonSchemaNull
   */
  def apply(): JsonSchemaNull.type = this
}
