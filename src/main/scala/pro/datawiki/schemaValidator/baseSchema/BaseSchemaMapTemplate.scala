package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.SparkRowElementTypeTemplate

import scala.collection.mutable

case class BaseSchemaMapTemplate(baseElement: BaseSchemaTemplate,
                                 inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    var locList: List[BaseSchemaStruct] = List.apply()
    val inObject: BaseSchemaObject = dataElement match
      case x: BaseSchemaObject => x
      case _ => throw Exception()


    inObject.getElements.foreach(element => {


      val mapElement = BaseSchemaObject(
        mutable.Map(
          ("key", BaseSchemaString(element._1, inIsIgnorable)),
          ("value", baseElement.extractDataFromObject(element._2))
        ),
        mutable.Map(
          ("key", BaseSchemaStringTemplate(inIsIgnorable)),
          ("value", baseElement)),
        inIsIgnorable
      )
      locList = locList.appended(mapElement)
    })
    return BaseSchemaArray(locList,
      baseElement,
      inIsIgnorable)
  }


  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    throw Exception()
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
  //    throw Exception()
  //  }
  //  override def isIgnorable: Boolean = inIsIgnorable

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    throw Exception()
  }

  override def getProjectSchema: SchemaTrait = {
    throw Exception()
  }
}
