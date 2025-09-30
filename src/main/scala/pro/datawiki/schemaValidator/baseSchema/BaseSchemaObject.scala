package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementStruct}
import pro.datawiki.schemaValidator.spark.{SparkConstructor, SparkRowAttribute}

case class BaseSchemaObject(inElements: List[(String, BaseSchemaStruct)],
                            inIsIgnorable: Boolean) extends BaseSchemaStruct {


  def appendElement(name: String, element: BaseSchemaStruct): BaseSchemaObject = {
    BaseSchemaObject(inElements ::: List.apply((name, element)), inIsIgnorable)
  }

  def getBaseSchemaStruct(name: String): BaseSchemaStruct = {
    inElements.filter(i => i._1 == name).foreach(col => return col._2)
    return null
  }


  def getElementStructByName(in: String, inSchema: BaseSchemaTemplate): BaseSchemaStruct = {
    val locValue: BaseSchemaStruct = getBaseSchemaStruct(in)
    if (locValue == null) then {
      inSchema match {
        case x: BaseSchemaStringTemplate => return BaseSchemaString("", true)
        case x: BaseSchemaObjectTemplate =>  return BaseSchemaObject(x.inElements.map(col => ((col._1,col._2.extractDataFromObject(null)))), true)
        case x: BaseSchemaArrayTemplate => return BaseSchemaArray(List.empty,x.baseElement,true)
        case _ => {
          throw NotImplementedException(s"Unsupported schema type for element extraction: ${inSchema.getClass.getName}")
        }
      }
    }
    try {
      return inSchema.extractDataFromObject(locValue)
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  def packageDataFrame: DataFrame = {
    try {
      return SparkConstructor.getDataFrameFromListBaseSchemaObjects(List(this), getTemplate)
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  override def getSparkRowElement: SparkRowElement = {
    val localList: List[SparkRowAttribute] = inElements.map(i => SparkRowAttribute(i._1, i._2.getSparkRowElement)).toList
    return SparkRowElementStruct(localList)
  }

  def getSparkRowAttributeBySchema(in: List[(String, BaseSchemaTemplate)]): List[SparkRowAttribute] = {
    try {
      in.map(i => {
        SparkRowAttribute(
          name = i._1,
          value =
            SparkRowElement(
              i._2.getSparkRowElementTemplate.getType,
              getElementStructByName(i._1, i._2).getSparkRowElement.getValue
            )
        )
      })
    } catch {
      case e: Exception => {
        throw e
      }
    }

  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaObjectTemplate = {
    try {
      return BaseSchemaObjectTemplate(inElements.map(col => {
        col._1 -> col._2.getTemplate
      }), inIsIgnorable)
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }
}
