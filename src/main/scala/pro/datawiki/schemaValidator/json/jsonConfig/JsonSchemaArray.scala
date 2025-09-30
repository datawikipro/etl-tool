package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JArray
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaArray, BaseSchemaNullTemplate, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет JSON массив в виде списка JSON элементов.
 */
case class JsonSchemaArray(inList: List[JsonStruct]) extends JsonStruct {

  /**
   * Преобразует JSON массив в структуру базовой схемы.
   *
   * @return Структура базовой схемы, представляющая массив.
   */
  override def getBaseSchemaElementData: BaseSchemaStruct = {
    //TODO ВАЖНО ИИ КОД
    // Преобразуем каждый элемент списка в базовую структуру схемы
    val structures = inList.map(_.getBaseSchemaElementData)
    if inList.isEmpty then {
      return BaseSchemaArray(structures, BaseSchemaNullTemplate(false), false)
    }
    // Начинаем с шаблона первого элемента (если список не пуст)
    val template = inList.headOption.map { firstElement =>
      // Создаем начальный шаблон из первого элемента
      val initialTemplate = firstElement.getBaseSchemaElementData.getTemplate

      // Объединяем шаблоны всех остальных элементов
      inList.tail.foldLeft(initialTemplate) { (accTemplate, element) =>
        accTemplate.fullMerge(element.getBaseSchemaElementData.getTemplate)
      }
    }.orNull

    BaseSchemaArray(structures, template, false)
  }

  def isEmpty: Boolean = inList.isEmpty
}
object JsonSchemaArray {
  /**
   * Создает JsonSchemaArray из JSON массива.
   *
   * @param in Исходный JSON массив.
   * @return Представление JSON массива в виде JsonSchemaArray.
   */
  def apply(in: JArray): JsonSchemaArray = {
    // Преобразуем каждый элемент JSON массива в JsonSchemaElement
    val elements = in.arr.map(JsonStruct.apply).toList
    new JsonSchemaArray(elements)
  }
}
