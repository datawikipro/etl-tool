package pro.datawiki.schemaValidator.projectSchema

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.json4s.JsonAST.{JArray, JField}
import org.json4s.{JObject, JValue}
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObjectTemplate, BaseSchemaTemplate}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class SchemaObject(
                         elements: List[SchemaElement] = List.apply()
                       ) extends SchemaTrait {
  @JsonIgnore
  def getBaseSchemaTemplate: BaseSchemaTemplate = {
    var list: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()
    elements.foreach(i => {
      list += (i.name, i.getBaseElement)
    })
    return new BaseSchemaObjectTemplate(list, false)
  }

  @JsonIgnore
  private def checkArray: Boolean = {
    elements.length match
      case 1 => {
        elements.head.getElementType match
          case SchemaType.Array => return true
          case _ => return LogMode.getDebugFalse
      }
      case _ => return LogMode.getDebugFalse
  }

  @JsonIgnore
  private def validateObjects(jFields: List[JField]): Boolean = {
    jFields.foreach(i => {
      if !checkElement(i) then return LogMode.getDebugFalse
    })
    return true
  }

  @JsonIgnore
  def validateJson(jsn: JValue): Boolean = {
    jsn match
      case x: JArray => {
        checkArray match
          case true => return elements.head.`array`.validateArray(x.arr)
          case false => return LogMode.getDebugFalse

      }
      case x: JObject => return validateObjects(x.obj)
      case _ => LogMode.getDebugFalse
  }

  @JsonIgnore
  private def checkElement(x: JField): Boolean = {
    elements.foreach(i => {
      if (i.name == x._1) then {
        if !i.checkElement(x._2) then return LogMode.getDebugFalse
        return true
      }
    })

    return LogMode.getDebugFalse

  }

}

object SchemaObject extends YamlClass {
  def apply(validatorConfigLocation: String): SchemaObject = {
    try {
      return mapper.readValue(getLines(validatorConfigLocation), classOf[SchemaObject])
    } catch {
      case e: com.fasterxml.jackson.core.JsonParseException =>
        throw SchemaValidationException(s"Ошибка синтаксиса при загрузке схемы из: $validatorConfigLocation - ${e.getMessage}", e)
      case e: com.fasterxml.jackson.databind.JsonMappingException =>
        throw SchemaValidationException(s"Ошибка структуры при загрузке схемы из: $validatorConfigLocation - ${e.getMessage}", e)
      case e: java.io.IOException =>
        throw SchemaValidationException(s"Ошибка ввода/вывода при загрузке схемы из: $validatorConfigLocation - ${e.getMessage}", e)
      case e: Exception =>
        throw SchemaValidationException(s"Непредвиденная ошибка при загрузке схемы валидатора из: $validatorConfigLocation - ${e.getMessage}", e)
    }
  }
}