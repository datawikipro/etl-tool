package pro.datawiki.schemaValidator.projectSchema

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.json4s.{JBool, JDouble, JInt, JString, JValue}
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class SchemaArray(
                        `object`: SchemaObject,
                        `type`: String
                      ) extends LogicClass, SchemaTrait {
  @JsonIgnore
  def getLogic: Any = {
    super.getLogic(`object`, `type`)
  }

  @JsonIgnore
  private def getBaseType(isIgnorable:Boolean): BaseSchemaTemplate = {
    getLogic match
      case null => return null
      case x: SchemaObject => return x.getBaseSchemaTemplate(isIgnorable)//TODO
      case x: String => BaseSchemaStringTemplate(isIgnorable)//TODO
      case _ => {
        throw NotImplementedException("Method not implemented")
      }
  }

  @JsonIgnore
  def getBaseArray(isIgnorable: Boolean): BaseSchemaArrayTemplate = {
    return BaseSchemaArrayTemplate(getBaseType(isIgnorable), isIgnorable)
  }

  @JsonIgnore
  def validateArray(in: List[JValue]): Boolean = {
    // Пустые массивы считаются валидными
    if (in == null || in.isEmpty) return true

    val logic = getLogic
    logic match {
      case schema: SchemaObject => validateObjectArray(in, schema)
      case typeStr: String => validatePrimitiveArray(in, typeStr)
      case _ => false
    }
  }

  /**
   * Валидация массива объектов
   */
  @JsonIgnore
  private def validateObjectArray(values: List[JValue], schema: SchemaObject): Boolean = {
    values.forall(value => schema.validateJson(value))
  }

  /**
   * Валидация массива примитивных типов
   */
  @JsonIgnore
  private def validatePrimitiveArray(values: List[JValue], typeStr: String): Boolean = {
    SchemaType(typeStr) match {
      case SchemaType.String => values.forall(_.isInstanceOf[JString])
      case SchemaType.Int | SchemaType.Long => values.forall(v => v.isInstanceOf[JInt] || v.isInstanceOf[JDouble])
      case SchemaType.Double => values.forall(v => v.isInstanceOf[JDouble] || v.isInstanceOf[JInt])
      case SchemaType.Boolean => values.forall(_.isInstanceOf[JBool])
      case _ => false
    }
  }

}
