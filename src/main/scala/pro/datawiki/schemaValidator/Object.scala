package pro.datawiki.schemaValidator

import org.json4s.{JBool, JObject, JString, JValue}
import org.json4s.JsonAST.{JArray, JField, JInt}



case class Object(
                   elements: List[Element]
                 ) {
  private def checkArray: Boolean = {
    elements.length match
      case 1 => {
        elements.head.elementType match
          case SchemaType.Array => return true
          case _ => return false
      }
      case _ => return false
  }

  def validateObjects(jFields: List[JField]): Unit = {
    jFields.foreach(i => checkElement(i))
  }

  def validateJson(jsn: JValue): Unit = {
    jsn match
      case x: JArray => {
        checkArray match
          case true => elements.head.array.processArray(x.arr)
          case false => {
            throw Exception()
          }
      }
      case x: JObject => {
        validateObjects(x.obj)
      }
      case _ => {
        throw Exception()
      }

  }
  private def checkElement(x: JField): Unit = {
    elements.foreach(i => {
      if (i.name == x._1) then {
        i.process(x._2)
        return
      }
    })
    throw Exception()
  }



}