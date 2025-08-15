package pro.datawiki.schemaValidator.json

import org.json4s.*
import org.json4s.jackson.JsonMethods.*
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.schemaValidator.json.jsonConfig.{JsonSchemaArrayTemplate, JsonSchemaBooleanTemplate, JsonSchemaDoubleTemplate, JsonSchemaErrorTemplate, JsonSchemaIntTemplate, JsonSchemaNullTemplate, JsonSchemaObjectTemplate, JsonSchemaStringTemplate}

import scala.collection.mutable

/**
 * Класс для преобразования структур BaseSchemaStruct в формат JSON Schema
 * согласно спецификации json-schema.org
 */
object JsonSchemaWriter {
  
  def convertToJsonSchema(schema: BaseSchemaTemplate, rootTitle: String = "Generated Schema", rootDescription: String = "Автоматически сгенерированная JSON схема"): String = {
    val jsonSchema = JsonSchemaWriter.generateJsonSchema(schema, rootTitle, rootDescription, true)
    pretty(render(jsonSchema))
  }

  def generateJsonSchema(schema: BaseSchemaTemplate, title: String = null, description: String = null, isShowSchema: Boolean): JValue = {
    // Базовая схема с общими атрибутами
    val list = isShowSchema match {
      case true => List(("$schema", JString("http://json-schema.org/draft-07/schema#")))
      case false => List()
    }

    val baseSchema: JObject = JObject(
      list :::
        (if (title != null) List("title" -> JString(title)) else Nil) :::
        (if (description != null) List("description" -> JString(description)) else Nil)
    )

    // Дополняем схему в зависимости от типа
    val schemaContent1: JsonSchemaTemplate = JsonSchemaTemplate(schema)
    val schemaContent: JValue = schemaContent1.generateJValue()
    // Объединяем базовую схему с конкретным содержимым
    return baseSchema merge schemaContent
  }

  /**
   * Генерирует схему для массива.
   *
   * @param schema Объект BaseSchemaArray
   * @return JValue, представляющий схему массива
   */
  private def generateArraySchema(schema: BaseSchemaArrayTemplate): JValue = {
    val itemSchema = schema.getBaseElement match {
      case objTemplate: BaseSchemaObjectTemplate =>
        // Создаем пустой объект схемы на основе шаблона
        val templateMap = mutable.Map[String, BaseSchemaTemplate]()
        objTemplate.getElements.foreach { case (name, template) =>
          templateMap += (name -> template)

        }
        val mockObj = new BaseSchemaObjectTemplate(templateMap, objTemplate.isIgnorable)
        JsonSchemaWriter.generateJsonSchema(schema = mockObj, isShowSchema = false)

      case strTemplate: BaseSchemaStringTemplate =>
        JsonSchemaWriter.generateJsonSchema(schema = new BaseSchemaStringTemplate(strTemplate.isIgnorable), isShowSchema = false)

      case intTemplate: BaseSchemaIntTemplate =>
        JsonSchemaWriter.generateJsonSchema(schema = new BaseSchemaIntTemplate(intTemplate.isIgnorable), isShowSchema = false)

      case doubleTemplate: BaseSchemaDoubleTemplate =>
        JsonSchemaWriter.generateJsonSchema(schema = new BaseSchemaDoubleTemplate(doubleTemplate.isIgnorable), isShowSchema = false)

      case boolTemplate: BaseSchemaBooleanTemplate =>
        JsonSchemaWriter.generateJsonSchema(schema = new BaseSchemaBooleanTemplate(boolTemplate.isIgnorable), isShowSchema = false)

      case _ => JObject("type" -> JString("string"))
    }

    JObject(
      "type" -> JString("array"),
      "items" -> itemSchema
    )
  }

  /**
   * Создает пустую структуру по шаблону для использования в генерации схемы.
   *
   * @param template Шаблон схемы
   * @return Пустая структура схемы, соответствующая шаблону
   */
  private def createEmptyStructFromTemplate(template: BaseSchemaTemplate): BaseSchemaTemplate = {
    template match {
      case objTemplate: BaseSchemaObjectTemplate => {
        val emptyElements = mutable.Map[String, BaseSchemaStruct]()
        val templateMap = mutable.Map[String, BaseSchemaTemplate]()
        objTemplate.getElements.foreach { case (name, subTemplate) =>
          templateMap += (name -> subTemplate)
        }
        new BaseSchemaObjectTemplate(templateMap, objTemplate.isIgnorable)
      }
      case strTemplate: BaseSchemaStringTemplate => new BaseSchemaStringTemplate(strTemplate.isIgnorable)
      case intTemplate: BaseSchemaIntTemplate => new BaseSchemaIntTemplate(intTemplate.isIgnorable)
      case doubleTemplate: BaseSchemaDoubleTemplate => new BaseSchemaDoubleTemplate(doubleTemplate.isIgnorable)
      case boolTemplate: BaseSchemaBooleanTemplate => new BaseSchemaBooleanTemplate(boolTemplate.isIgnorable)
      case nullTemplate: BaseSchemaNullTemplate => BaseSchemaNullTemplate(nullTemplate.isIgnorable)
      case arrayTemplate: BaseSchemaArrayTemplate => new BaseSchemaArrayTemplate(arrayTemplate.getBaseElement, arrayTemplate.isIgnorable)
      case _ => BaseSchemaNullTemplate(false)
    }
  }

}
