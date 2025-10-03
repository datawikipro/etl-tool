package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JObject
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

import scala.collection.mutable

/**
 * Представляет JSON объект в виде коллекции пар ключ-значение,
 * где значения являются элементами JSON схемы.
 *
 * @param elements Карта элементов JSON объекта (поле-значение)
 */
class JsonSchemaObject(elements: mutable.Map[String, JsonStruct]) extends JsonStruct {
  override def getBaseSchemaElementData: BaseSchemaStruct = {
    // Преобразуем каждый элемент объекта в базовую структуру
    val structMap = elements.map { case (fieldName, jsonElement) =>
      (fieldName, jsonElement.getBaseSchemaElementData)
    }.toList

    BaseSchemaObject(structMap, false)
  }
  def isEmpty:Boolean={
    elements.forall(col=>col._2.isEmpty)
  }

  def mergeSchema(schemaObject: BaseSchemaObject): BaseSchemaObject = {
    // TODO: Реализовать объединение схем
    throw new Exception("Метод mergeSchema не реализован")
  }
}

object JsonSchemaObject {

  def apply(json: JObject): JsonSchemaObject = {
    val elementMap = json.obj.filterNot { case (_, fieldValue) =>
      fieldValue.toString == "JNull"
    }.foldLeft(mutable.Map.empty[String, JsonStruct]) {
      case (map, (fieldName, fieldValue)) =>
        map += (fieldName -> JsonStruct(fieldValue))
    }.filterNot(col => col._2.isEmpty)
    new JsonSchemaObject(elementMap)
  }
}

