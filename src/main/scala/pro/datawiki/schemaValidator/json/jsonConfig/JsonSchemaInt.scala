package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JInt
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaInt, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет целочисленное значение в JSON схеме.
 *
 * @param value Целочисленное значение
 */
case class JsonSchemaInt(value: Long) extends JsonStruct {
  /**
   * Преобразует целочисленное значение в структуру базовой схемы.
   *
   * @return Структура базовой схемы для целочисленного значения
   */
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaInt(value, false)

  override def isEmpty: Boolean = false
}

object JsonSchemaInt {
  /**
   * Создает JsonSchemaInt из JSON целочисленного значения.
   *
   * @param jInt JSON целочисленное значение
   * @return Представление JSON числа в виде JsonSchemaInt
   */
  def apply(jInt: JInt): JsonSchemaInt = JsonSchemaInt(jInt.num.toLong)
}
