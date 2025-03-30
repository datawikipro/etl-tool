package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JArray, JField, JInt, JNull}
import org.json4s.{JBool, JObject, JString, JValue}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.parent.LogicClass

case class SchemaElement(
                          name: String,
                          `type`: String,
                          `array`: SchemaArray,
                          `object`: SchemaObject,
                          `map`: SchemaMap,
                          isIgnorable:Boolean = false
                  ) extends LogicClass {
  def getBaseElement:BaseSchemaTemplate = {
    val logic = getLogic
    logic match
      case x: String => {
        x match
          case "String" => return BaseSchemaStringTemplate(isIgnorable)
          case "Int" => return BaseSchemaIntTemplate(isIgnorable)
          case "Boolean" => return BaseSchemaBoolean(false,isIgnorable)
          case _ => {
            throw Exception()
          }
      }
      case x: SchemaArray => return `array`.getBaseArray
      case x: SchemaObject => return `object`.getBaseObject
      case x: SchemaMap => return `map`.getBaseMap
      case _ =>
        throw Exception()
  }


  override def getLogic: Any = {
    reset()
    setLogic(`type`)
    setLogic(`array`)
    setLogic(`object`)
    setLogic(`map`)
    super.getLogic
  }

  def getElementType: SchemaType = {
    val logic = getLogic
    logic match
      case x: String => {
        x match
          case "String" => return SchemaType.String
          case "Int" => return SchemaType.Int
          case "Boolean" => return SchemaType.Boolean
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

  def checkElement(jsn: JValue): Boolean = {
    jsn match
      case x: JObject => {
        if getElementType == SchemaType.Object then {
          getLogic match
            case y: SchemaObject => y.validateJson(x)
            case y: SchemaMap => y.validateJson(x)
            case _=> LogMode.getDebugFalse
          return true
        } else {
          return LogMode.getDebugFalse
        }
      }
      case x: JInt =>
        getElementType match
          case SchemaType.Int => return true
          case SchemaType.String => return true
          case _ => {
            return LogMode.getDebugFalse
          }
      case x: JString =>
        getElementType match
          case SchemaType.String => return true
          case _ => LogMode.getDebugFalse
      case x: JBool =>
        val a  = getElementType

        getElementType match
          case SchemaType.Boolean => return true
          case SchemaType.String => return true
          case _ =>             return LogMode.getDebugFalse
          
      case x: JArray => {
        val elementType = getElementType
        elementType match
          case SchemaType.Array => {
            `array`.validateArray(x.arr)
            return true
          }
          case _ =>      LogMode.getDebugFalse
          
      }
      case _ => {
        if jsn.toString == "JNull" then return true
        return LogMode.getDebugFalse
      }
  }

}