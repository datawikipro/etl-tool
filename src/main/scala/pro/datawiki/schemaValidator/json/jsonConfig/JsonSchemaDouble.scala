package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JDouble
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaDouble, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет значение с плавающей точкой в JSON схеме.
 *
 * @param value Значение с плавающей точкой
 */
case class JsonSchemaDouble(value: Double) extends JsonStruct {
  /**
   * Преобразует значение с плавающей точкой в структуру базовой схемы.
   *
   * @return Структура базовой схемы для значения с плавающей точкой
   */
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaDouble(value, false)

  override def isEmpty: Boolean = false
}

object JsonSchemaDouble {
  /**
   * Создает JsonSchemaDouble из JSON значения с плавающей точкой.
   *
   * @param jDouble JSON значение с плавающей точкой
   * @return Представление JSON числа в виде JsonSchemaDouble
   */
  def apply(jDouble: JDouble): JsonSchemaDouble = JsonSchemaDouble(jDouble.num)
}
