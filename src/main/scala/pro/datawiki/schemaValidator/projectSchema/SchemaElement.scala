package pro.datawiki.schemaValidator.projectSchema

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.{JBool, JDouble, JObject, JString, JValue}
import pro.datawiki.exception.{NotImplementedException, SchemaValidationException}
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class SchemaElement(
                          name: String,
                          `type`: String,
                          `array`: SchemaArray,
                          `object`: SchemaObject,
                          `map`: SchemaMap,
                          isIgnorable: Boolean = false
                        ) extends SchemaTrait {
  @JsonIgnore
  def getBaseElement: BaseSchemaTemplate = {
    val logic = getLogic
    logic match {
      case x: String => createPrimitiveTemplate(x, isIgnorable)
      case _: SchemaArray => `array`.getBaseArray(isIgnorable)
      case _: SchemaObject => `object`.getBaseSchemaTemplate(isIgnorable)
      case _: SchemaMap => `map`.getBaseMap(isIgnorable)
      case _ => throw NotImplementedException(s"Unsupported logic type: ${logic.getClass.getSimpleName}")
    }
  }

  @JsonIgnore
  private def createPrimitiveTemplate(typeStr: String, ignorable: Boolean): BaseSchemaTemplate = {
    SchemaType(typeStr) match {
      case SchemaType.String => BaseSchemaStringTemplate(ignorable)
      case SchemaType.Int | SchemaType.Long => BaseSchemaIntTemplate(ignorable)
      case SchemaType.Boolean => BaseSchemaBooleanTemplate(ignorable)
      case SchemaType.Double => BaseSchemaDoubleTemplate(ignorable)
      case SchemaType.Null => BaseSchemaNullTemplate(ignorable)
      case _ => throw NotImplementedException(s"Unsupported primitive type: $typeStr")
    }
  }

  @JsonIgnore
  def getLogic: Any = {
    LogicClass.getLogic(`type`, `array`, `object`, `map`)
  }

  @JsonIgnore
  def getElementType: SchemaType = {
    val logic = getLogic
    logic match {
      case x: String => SchemaType(x)
      case _: SchemaArray => SchemaType.Array
      case _: SchemaObject => SchemaType.Object
      case _: SchemaMap => SchemaType.Object
      case _ => throw NotImplementedException(s"Unsupported logic type: ${logic.getClass.getSimpleName}")
    }
  }

  @JsonIgnore
  def checkElement(jsn: JValue): Boolean = {
    val elementType = getElementType

    jsn match {
      case x: JObject => validateObject(x, elementType)
      case x: JInt => validateNumeric(x, elementType)
      case x: JDouble => validateNumeric(x, elementType)
      case x: JString => validateString(x, elementType)
      case x: JBool => validateBoolean(x, elementType)
      case x: JArray => validateArray(x, elementType)
      case _ => validateNull(jsn, elementType)
    }
  }

  @JsonIgnore
  private def validateObject(jsn: JObject, elementType: SchemaType): Boolean = {
    elementType match {
      case SchemaType.Object =>
        getLogic match {
          case schema: SchemaObject => schema.validateJson(jsn)
          case map: SchemaMap => map.validateJson(jsn)
          case _ => false
        }
      case _ => false
    }
  }

  @JsonIgnore
  private def validateNumeric(jsn: JValue, elementType: SchemaType): Boolean = {
    elementType match {
      case SchemaType.Int | SchemaType.Long | SchemaType.Double => true
      case SchemaType.String => true // Числа могут быть преобразованы в строки
      case _ => false
    }
  }

  @JsonIgnore
  private def validateString(jsn: JString, elementType: SchemaType): Boolean = {
    elementType match {
      case SchemaType.String => true
      case SchemaType.Int | SchemaType.Long | SchemaType.Double => true // Строки могут быть преобразованы в числа
      case SchemaType.Boolean => true // Строки могут быть преобразованы в булевы значения
      case _ => false
    }
  }

  @JsonIgnore
  private def validateBoolean(jsn: JBool, elementType: SchemaType): Boolean = {
    elementType match {
      case SchemaType.Boolean => true
      case SchemaType.String => true // Булевы значения могут быть преобразованы в строки
      case _ => false
    }
  }

  @JsonIgnore
  private def validateArray(jsn: JArray, elementType: SchemaType): Boolean = {
    elementType match {
      case SchemaType.Array => `array`.validateArray(jsn.arr)
      case _ => false
    }
  }

  @JsonIgnore
  private def validateNull(jsn: JValue, elementType: SchemaType): Boolean = {
    if (jsn.toString == "JNull") {
      elementType match {
        case SchemaType.Null => true
        case _ if isIgnorable => true // Игнорируемые поля могут быть null
        case _ => false
      }
    } else {
      false
    }
  }

}