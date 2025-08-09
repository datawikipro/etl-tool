package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.types.{DataType, StructField}
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.SchemaValidator.toYaml
import pro.datawiki.schemaValidator.dataFrame.DataFrameSchemaValidator
import pro.datawiki.schemaValidator.projectSchema.{SchemaElement, SchemaObject, SchemaTrait, SchemaType}
import pro.datawiki.schemaValidator.sparkRow.{SparkRowAttributeTemplate, SparkRowElementStructTemplate, SparkRowElementTypeTemplate}

import scala.collection.mutable

class BaseSchemaObjectTemplate(inElements: mutable.Map[String, BaseSchemaTemplate],
                               inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  
  def getElements: mutable.Map[String, BaseSchemaTemplate] = inElements
  def getElementByName(in: String): BaseSchemaTemplate = {
    val b = inElements.get(in)
    if b == null then return BaseSchemaNullTemplate(inIsIgnorable)
    if b.isEmpty then return BaseSchemaNullTemplate(inIsIgnorable)
    return b.get
  }

  override def extractDataFromObject(in: BaseSchemaStruct): BaseSchemaStruct = {
    var newElements: mutable.Map[String, BaseSchemaStruct] = mutable.Map()

    val inObject: BaseSchemaObject = in match
      case x: BaseSchemaObject => x
      case x: BaseSchemaNull =>
        return BaseSchemaObject(newElements, inElements, inIsIgnorable)
      case _ => throw Exception()

    inElements.foreach(i => {
      newElements += (i._1, i._2.extractDataFromObject(inObject.getElementStructByName(i._1)))
    })
    return BaseSchemaObject(newElements, inElements, inIsIgnorable)
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    var newElements: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()

    val inObject = in match
      case x: BaseSchemaObjectTemplate => x
      case x: BaseSchemaNullTemplate => new BaseSchemaObjectTemplate(inElements, inIsIgnorable)
      case null => return new BaseSchemaObjectTemplate(inElements, inIsIgnorable)
      case other => {
        throw SchemaValidationException(s"Несовместимый тип шаблона для слияния с шаблоном объекта: ${other.getClass.getName}")
      }

    inElements.foreach(i => {
      newElements += (i._1, i._2.fullMerge(inObject.getElementByName(i._1)))
    })
    return new BaseSchemaObjectTemplate(newElements, inIsIgnorable)
  }


  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    var localList: List[SparkRowAttributeTemplate] = List.apply()
    inElements.foreach(i => {
      localList = localList.appended(SparkRowAttributeTemplate(i._1, i._2.getSparkRowElementTemplate))
    })
    return SparkRowElementStructTemplate(localList)
  }


  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    var newElements: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()

    val inObject = in match
      case x: BaseSchemaObjectTemplate => x
      case x: BaseSchemaNullTemplate => return new BaseSchemaObjectTemplate(inElements, inIsIgnorable)
      case other => throw SchemaValidationException(s"Невозможно извлечь данные объекта из: ${other.getClass.getName}")

    inElements.foreach(i => {
      newElements += (i._1, i._2.fullMerge(inObject.getElementByName(i._1)))
    })

    inObject.getElements.foreach(i => {
      newElements += (i._1, i._2.fullMerge(this.getElementByName(i._1)))
    })
    
    return new BaseSchemaObjectTemplate(newElements, inIsIgnorable)
  }

  override def getProjectSchema: SchemaObject = {
    var list: List[SchemaElement] = List.apply()
    inElements.foreach(i=> {
      i._2 match
        case x: BaseSchemaStringTemplate => list = list.appended(SchemaElement(name = i._1, `type` = SchemaType.String.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaNullTemplate => list = list.appended(SchemaElement(name = i._1, `type` = SchemaType.Null.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaObjectTemplate => list = list.appended(SchemaElement(name = i._1, `type` = null, `array` = null, `object` = x.getProjectSchema, `map` = null, isIgnorable = false))
        case x: BaseSchemaIntTemplate => list = list.appended(SchemaElement(name = i._1, `type` = SchemaType.Int.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaDoubleTemplate => list = list.appended(SchemaElement(name = i._1, `type` = SchemaType.Double.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaBooleanTemplate => list = list.appended(SchemaElement(name = i._1, `type` = SchemaType.Boolean.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaArrayTemplate => list = list.appended(SchemaElement(name = i._1, `type` = null, `array` = x.getProjectSchema, `object` = null, `map` = null, isIgnorable = false))
        
        case _ => {
          throw Exception()
        }
    })
    return SchemaObject(list)
  }
}


object BaseSchemaObjectTemplate {
  def apply(in: List[StructField]): BaseSchemaObjectTemplate = {
    val templates = mutable.Map[String, BaseSchemaTemplate]()
    val isIgnorable = false

    // Преобразуем каждое StructField в соответствующий BaseSchemaTemplate
    in.foreach(field => {
      val template = DataFrameSchemaValidator.convertStructFieldToTemplate(field)
      templates += (field.name -> template)
    })

    val schema =new BaseSchemaObjectTemplate(templates, isIgnorable)
    val project = toYaml(schema.getProjectSchema)
    return schema
  }
}