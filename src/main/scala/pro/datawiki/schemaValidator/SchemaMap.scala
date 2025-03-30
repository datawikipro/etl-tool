package pro.datawiki.schemaValidator

import org.json4s.JValue
import org.json4s.JsonAST.JObject
import pro.datawiki.sparkLoader.LogMode

case class SchemaMap(
                `object`: SchemaObject
              ) {
  
  def getBaseMap:BaseSchemaMapTemplate = {
    if `object` == null then
      return BaseSchemaMapTemplate(null,false)
    return BaseSchemaMapTemplate(`object`.getBaseObject,false)
  }
  
  def validateJson(jsn: JValue): Boolean = {
    jsn match
      case x: JObject => {
        x.obj.foreach(i => `object`.validateJson(i._2))
        return true
      }
      case _ => LogMode.getDebugFalse
  }
}
