package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JArray, JField, JInt, JNull}
import org.json4s.{JBool, JObject, JString, JValue}
import pro.datawiki.sparkLoader.configuration.parent.LogicClass

case class Element(
                    name: String,
                    `type`: String,
                    array: Array,
                    `object`: Object
                  ) extends LogicClass {
  def elementType: SchemaType = {
    reset()
    setLogic(`type`)
    setLogic(array)
    setLogic(`object`)
    super.getLogic match
      case x: String => {
        x match
          case "String" => return SchemaType.String
          case "Int" => return SchemaType.Int
          case "Boolean" => return SchemaType.Boolean
          case _ => {
            throw Exception()
          }
      }
      case x: Array => return SchemaType.Array
      case x: Object => return SchemaType.Object
      case _ => throw Exception()
  }

  def process(jsn: JValue): Unit = {
    jsn match
      case x: JObject => {
        if elementType == SchemaType.Object then {
          `object`.validateJson(x)
          return
        } else {
          throw Exception()
        }
      }
      case x: JInt =>
        elementType match
          case SchemaType.Int => return
          case _ => {
            throw Exception()
          }
      case x: JString =>
        elementType match
          case SchemaType.String => return
          case _ => {
            throw Exception()
          }
      case x: JBool =>
        elementType match
          case SchemaType.Boolean => return
          case _ => {
            throw Exception()
          }
      case x: JArray => {
        elementType match
          case SchemaType.Array => {
            array.processArray(x.arr)
            return
          }
          case _ => {
            throw Exception()
          }
      }
      case _ => {
        if jsn.toString == "JNull" then return
        throw Exception()
      }
  }



}