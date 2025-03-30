package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import org.json4s.{JBool, JObject, JString, JValue}
import org.json4s.JsonAST.{JArray, JField, JInt}
import pro.datawiki.sparkLoader.LogMode

import scala.collection.mutable

case class SchemaObject(
                         elements: List[SchemaElement] = List.apply()
                       ){

  def getBaseObject:BaseSchemaTemplate={
    var list: mutable.Map[String, BaseSchemaTemplate] =  mutable.Map()
    elements.foreach( i=> {
        list += (i.name, i.getBaseElement)
      }
    )
    return BaseSchemaObjectTemplate(list,false)
  }

  private def checkArray: Boolean = {
    elements.length match
      case 1 => {
        elements.head.getElementType match
          case SchemaType.Array => return true
          case _ => return LogMode.getDebugFalse
      }
      case _ => return LogMode.getDebugFalse
  }

  private def validateObjects(jFields: List[JField]): Boolean = {
    jFields.foreach(i => {
      if !checkElement(i) then return LogMode.getDebugFalse
    })
    return true
  }

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

