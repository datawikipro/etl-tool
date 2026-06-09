package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.exception.{NotImplementedException, SchemaValidationException}
import pro.datawiki.schemaValidator.projectSchema.SchemaMap
import pro.datawiki.schemaValidator.spark.SparkRowAttributeTemplate
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElement
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementListTemplate, SparkRowElementStringTemplate, SparkRowElementStructTemplate, SparkRowElementTypeTemplate}

case class BaseSchemaMapTemplate(baseElement: BaseSchemaTemplate,
                                 inIsIgnorable: Boolean) extends BaseSchemaTemplate {

  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    val inObject: BaseSchemaObject =
      dataElement match
        case x: BaseSchemaObject => x
        case _ => throw NotImplementedException("Method not implemented")

    val locList: List[BaseSchemaStruct] =
      inObject.inElements.map(element => {
        BaseSchemaObject(
          List.apply(
            ("key", BaseSchemaString(element._1, inIsIgnorable)),
            ("value", baseElement.extractDataFromObject(element._2))
          ),
          inIsIgnorable
        )
      }).toList
    return BaseSchemaArray(locList, baseElement, inIsIgnorable)
  }


  override def leftValidate(in: BaseSchemaTemplate): Boolean = {
    
    //TODO
    throw NotImplementedException("Method not implemented")
  }
  //  def getSubElement: BaseSchemaStruct = baseElement
  //
  //  override def equals(in: BaseSchemaStruct): Boolean = {
  //    in match
  //      case x: BaseSchemaObject => {
  //        x.getElements.foreach(i => {
  //          if !i._2.equals(baseElement) then return LogMode.getDebugFalse
  //        })
  //        return true
  //      }
  //      case _ => return LogMode.getDebugFalse
  //  }
  //
  //
  //  def getSparkRowElement: SparkRowElementType = {
  //    throw NotImplementedException("Method not implemented")
  //  }
  //  override def isIgnorable: Boolean = inIsIgnorable

  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    throw NotImplementedException("fullMerge not implemented for BaseSchemaMapTemplate")
  }
  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    if baseElement == null then return {
      return SparkRowElementListTemplate(baseType = SparkRowElementStringTemplate())
    }


    baseElement.getSparkRowElementTemplate match
      case x: SparkRowElementTypeTemplate => {
        return SparkRowElementListTemplate(
          baseType = SparkRowElementStructTemplate(
            List.apply(
              SparkRowAttributeTemplate(name = "key", value = SparkRowElementStringTemplate()),
              SparkRowAttributeTemplate(name = "value", value = x)
            )))
      }
      case _ => {
        throw NotImplementedException("Method not implemented")
      }
  }

  override def getProjectSchema: SchemaMap = {
    baseElement match
      case x: BaseSchemaObjectTemplate => return SchemaMap(`object` = x.getProjectSchema)
      case other => {
        throw SchemaValidationException(s"Несовместимый тип шаблона для слияния с шаблоном словаря: ${other.getClass.getName}")
      }
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def equals(in: BaseSchemaTemplate): Boolean = {
    throw NotImplementedException("equals not implemented for BaseSchemaMapTemplate")
  }
  override def getSparkRowElement(data: BaseSchemaStruct): SparkRowElement = {
    throw NotImplementedException("getSparkRowElement not implemented for BaseSchemaMapTemplate")
  }
}
