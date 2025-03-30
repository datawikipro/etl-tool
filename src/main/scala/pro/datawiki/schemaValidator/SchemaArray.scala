package pro.datawiki.schemaValidator

import org.json4s.{JString, JValue}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.parent.LogicClass

case class SchemaArray(
                  `object`: SchemaObject,
                  `type`: String
                ) extends LogicClass{

  override def getLogic: Any = {
    reset()
    setLogic(`object`)
    setLogic(`type`)
    super.getLogic
  }

  private def getBaseType: BaseSchemaTemplate = {
    getLogic match
      case null => return null
      case x: SchemaObject => return x.getBaseObject
      case x: String => x match
        case "String" =>  return  BaseSchemaStringTemplate(false)
        case "Int" => return BaseSchemaIntTemplate(false)
        case "Boolean" => return  BaseSchemaBooleanTemplate(false)
        case _ => throw Exception()
  }

  def getBaseArray:BaseSchemaArrayTemplate = {
    return BaseSchemaArrayTemplate(getBaseType,false)
  }

  def validateArray(in: List[JValue]): Boolean = {
    if in == null then return true
    if in.isEmpty then return true
    val logic = getLogic
    logic match
      case x:SchemaObject => {in.foreach(i => {
        if x.validateJson(i) then {
          return true
        }
      })}
      case x: String => x match
        case "String" => {in.foreach(i => {
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
