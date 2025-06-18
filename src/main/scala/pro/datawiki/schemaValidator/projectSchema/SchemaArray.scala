package pro.datawiki.schemaValidator.projectSchema

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.json4s.{JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class SchemaArray(
                        `object`: SchemaObject,
                        `type`: String
                      ) extends LogicClass,SchemaTrait {
  @JsonIgnore
  def getLogic: Any = {
    super.getLogic(`object`,`type`)
  }
  @JsonIgnore
  private def getBaseType: BaseSchemaTemplate = {
    getLogic match
      case null => return null
      case x: SchemaObject => return x.getBaseObject
      case x: String => x match
        case "String" => return BaseSchemaStringTemplate(false)
        case "Int" => return BaseSchemaIntTemplate(false)
        case "Boolean" => return BaseSchemaBooleanTemplate(false)
        case _ => throw Exception()
  }
  @JsonIgnore
  def getBaseArray: BaseSchemaArrayTemplate = {
    return BaseSchemaArrayTemplate(getBaseType, false)
  }

  @JsonIgnore
  def validateArray(in: List[JValue]): Boolean = {
    if in == null then return true
    if in.isEmpty then return true
    val logic = getLogic
    logic match
      case x: SchemaObject => {
        in.foreach(i => {
          if x.validateJson(i) then {
            return true
          }
        })
      }
      case x: String => x match
        case "String" => {
          in.foreach(i => {
            i match
              case x: JString =>
              case _ => LogMode.getDebugFalse
          })
          return true
        }
        case _ => LogMode.getDebugFalse
      case _ => LogMode.getDebugFalse


    return LogMode.getDebugFalse
  }

}
