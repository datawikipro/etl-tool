package pro.datawiki.schemaValidator.projectSchema

import com.fasterxml.jackson.annotation.JsonIgnore
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaMapTemplate
import pro.datawiki.sparkLoader.LogMode

case class SchemaMap(
                      `object`: SchemaObject
                    ) extends SchemaTrait {
  @JsonIgnore
  def getBaseMap(isIgnorable: Boolean): BaseSchemaMapTemplate = {
    if `object` == null then
      return BaseSchemaMapTemplate(null, isIgnorable)
    return BaseSchemaMapTemplate(`object`.getBaseSchemaTemplate(isIgnorable), isIgnorable)
  }

  @JsonIgnore
  def validateJson(jsn: JValue): Boolean = {
    jsn match {
      case x: JObject =>
        if (`object` == null) {
          true // Если схема объекта не определена, считаем валидным
        } else {
          x.obj.forall(field => `object`.validateJson(field._2))
        }
      case _ => false
    }
  }
}
