package pro.datawiki.schemaValidator

import pro.datawiki.sparkLoader.LogMode

class BaseSchemaArray(list: List[BaseSchemaStruct],
                      baseElement: BaseSchemaTemplate,
                      inIsIgnorable: Boolean) extends BaseSchemaArrayTemplate(baseElement,  inIsIgnorable) ,BaseSchemaStruct {

  def getList: List[BaseSchemaStruct] = list

  def getSubElement: BaseSchemaTemplate = baseElement
  
//
//  override def equals(in: BaseSchemaStruct): Boolean = {
//    in match
//      case x: BaseSchemaArray => {
//        val baseIn = x.getSubElement
//
//        val inEmpty: Boolean = baseIn match
//          case null => true
//          case x: BaseSchemaObject => throw Exception()//if x.inElements.isEmpty then true else false
//          case x: BaseSchemaString => false
//          case _ => LogMode.getDebugFalse
//
//        val baseEmpty: Boolean = baseElement match
//          case null => true
//          case x: BaseSchemaObject => throw Exception()//if x.inElements.isEmpty then true else false
//          case x: BaseSchemaString => false
//          case _ => LogMode.getDebugFalse
//
//        if inEmpty then return true
//
//        if baseEmpty then return true //LogMode.getDebugFalse//TODO
//
//        //MAIN DEBUG
//        return baseIn.equals(baseElement)
//      }
//      case x: BaseSchemaNull => return true // TODO
//
//      case _ => return LogMode.getDebugFalse
//  }

  //  
  //  override def mergeSchema(schemaObject: BaseSchemaElement): BaseSchemaElement = {
  //    var locList: List[BaseSchemaElement] = List.apply()
  //    schemaObject match
  //      case x: BaseSchemaArray => {
  //        throw Exception()
  ////        x.getList.foreach(i => locList = locList.appended(baseElement.mergeSchema(i)))
  ////
  ////        return BaseSchemaArray(locList, baseElement,schemaObject.isIgnorable)
  //      }
  //      case x: BaseSchemaNull => return BaseSchemaArray(locList, baseElement,schemaObject.isIgnorable)
  //      case _ => throw Exception()
  //  }

  override def getSparkRowElement: SparkRowElementList = {
    var l: List[SparkRowElementRow] = List.apply()
    list.foreach(i => {
      val row = i.getSparkRowElement

      row match
        case x: SparkRowElementStruct => l = l.appended(SparkRowElementRow(List.apply(SparkRowAttribute("data", x))))
        case x: SparkRowElementString => l = l.appended(SparkRowElementRow(List.apply(SparkRowAttribute("data", x))))
        case _ =>
          throw Exception()
    })

    return SparkRowElementList(l.toSeq)
  }

  override def isIgnorable: Boolean = inIsIgnorable
}

object BaseSchemaArray {
  def applyStruct(list: List[BaseSchemaStruct],
                  baseElement: BaseSchemaTemplate,
                  inIsIgnorable: Boolean): BaseSchemaStruct = {
    return new BaseSchemaArray(list, baseElement, inIsIgnorable)
  }

  def applyTemplate(list: List[BaseSchemaStruct],
                    baseElement: BaseSchemaTemplate,
                    inIsIgnorable: Boolean): BaseSchemaTemplate = {
    return new BaseSchemaArray(list, baseElement, inIsIgnorable)
  }

  
  

}