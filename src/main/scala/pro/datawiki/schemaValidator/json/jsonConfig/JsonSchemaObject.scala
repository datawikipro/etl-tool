package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JObject
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.json.JsonStruct

import scala.collection.mutable

/**
 * Представляет JSON объект в виде коллекции пар ключ-значение,
 * где значения являются элементами JSON схемы.
 *
 * @param elements Карта элементов JSON объекта (поле-значение)
 */
class JsonSchemaObject(elements: mutable.Map[String, JsonStruct]) extends JsonStruct {
  /**
   * Преобразует JSON объект в структуру базовой схемы.
   *
   * @return Структура базовой схемы, представляющая объект
   */
  override def getBaseSchemaElementData: BaseSchemaStruct = {
    // Преобразуем каждый элемент объекта в базовую структуру
    val structMap = elements.map { case (fieldName, jsonElement) =>
      val baseStruct = jsonElement.getBaseSchemaElementData
      (fieldName, baseStruct)
    }

    // Создаем карту шаблонов для каждого поля
    val templateMap = structMap.map { case (fieldName, baseStruct) =>
      (fieldName, baseStruct.getTemplate)
    }

    BaseSchemaObject(structMap, templateMap, false)
  }

  /**
   * Объединяет текущий объект с другим объектом схемы.
   * 
   * @param schemaObject Объект схемы для объединения
   * @return Объединенный объект схемы
   * @throws Exception текущая реализация не поддерживает объединение схем
   */
  def mergeSchema(schemaObject: BaseSchemaObject): BaseSchemaObject = {
    // TODO: Реализовать объединение схем
    throw new Exception("Метод mergeSchema не реализован")
  }
}

object JsonSchemaObject {
  /**
   * Создает JsonSchemaObject из JSON объекта.
   *
   * @param json JSON объект для преобразования
   * @return Представление JSON объекта в виде JsonSchemaObject
   */
  def apply(json: JObject): JsonSchemaObject = {
    // Преобразуем каждое поле JSON объекта в соответствующий элемент схемы
    val elementMap = json.obj.foldLeft(mutable.Map.empty[String, JsonStruct]) { 
      case (map, (fieldName, fieldValue)) =>
        map += (fieldName -> JsonStruct(fieldValue))
    }

    new JsonSchemaObject(elementMap)
  }
}

