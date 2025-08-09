package pro.datawiki.schemaValidator.projectSchema

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.{JBool, JDouble, JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaBooleanTemplate, BaseSchemaDoubleTemplate, BaseSchemaIntTemplate, BaseSchemaStringTemplate, BaseSchemaTemplate}
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
                        ) extends LogicClass, SchemaTrait {
  @JsonIgnore
  def getBaseElement: BaseSchemaTemplate = {
    val logic = getLogic
    logic match
      case x: String => {
        x match
          case "String" => return BaseSchemaStringTemplate(isIgnorable)
          case "Int" => return BaseSchemaIntTemplate(isIgnorable)
          case "Long" => return BaseSchemaIntTemplate(isIgnorable)
          case "Boolean" => return BaseSchemaBooleanTemplate(isIgnorable)
          case "Double" => return BaseSchemaDoubleTemplate(isIgnorable)
          case _ => {
            throw Exception()
          }
      }
      case x: SchemaArray => return `array`.getBaseArray
      case x: SchemaObject => return `object`.getBaseSchemaTemplate
      case x: SchemaMap => return `map`.getBaseMap
      case _ =>
        throw Exception()
  }

  @JsonIgnore
  def getLogic: Any = {
    super.getLogic(`type`, `array`, `object`, `map`)
  }

  @JsonIgnore
  def getElementType: SchemaType = {
    val logic = getLogic
    logic match
      case x: String => {
        x match
          case "String" => return SchemaType.String
          case "Int" => return SchemaType.Int
          case "Long" => return SchemaType.Long
          case "Boolean" => return SchemaType.Boolean
          case "Double" => return SchemaType.Double
          case _ => {
            throw Exception()
          }
      }
      case x: SchemaArray => return SchemaType.Array
      case x: SchemaObject => return SchemaType.Object
      case x: SchemaMap => return SchemaType.Object
      case _ =>
        throw Exception()
  }

  @JsonIgnore
  def checkElement(jsn: JValue): Boolean = {
    jsn match
      case x: JObject => {
        getElementType match
          case SchemaType.Object => {
            getLogic match
              case y: SchemaObject => y.validateJson(x)
              case y: SchemaMap => y.validateJson(x)
              case _ => LogMode.getDebugFalse
            return true
          }
          case _ => return LogMode.getDebugFalse

      }
      case x: JInt =>
        getElementType match
          case SchemaType.Int => return true
          case SchemaType.Long => return true
          case SchemaType.String => return true
          case _ => {
            return LogMode.getDebugFalse
          }
      case x: JDouble =>
        getElementType match
          case SchemaType.Int => return true
          case SchemaType.String => return true
          case SchemaType.Double => return true
          case _ => {
            return LogMode.getDebugFalse
          }

      case x: JString =>
        getElementType match
          case SchemaType.String => return true
          case _ => LogMode.getDebugFalse
      case x: JBool =>
        val a = getElementType

        getElementType match
          case SchemaType.Boolean => return true
          case SchemaType.String => return true
          case _ => return LogMode.getDebugFalse

      case x: JArray => {
        val elementType = getElementType
        elementType match
          case SchemaType.Array => {
            `array`.validateArray(x.arr)
            return true
          }
          case _ => LogMode.getDebugFalse

      }
      case _ => {
        if jsn.toString == "JNull" then return true
        return LogMode.getDebugFalse
      }
  }

}