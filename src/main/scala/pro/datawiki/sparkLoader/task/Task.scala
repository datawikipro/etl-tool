package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFramePartition, DataFrameTrait}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

trait Task {
  def run(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): Boolean

  def setCache(in: TransformationCacheTrait): Unit
}

object Task {
  private var views: mutable.Map[String, DataFrameTrait] = mutable.Map()
  
  def getView(in:String):DataFrameTrait = {
    val view: Option[DataFrameTrait] = views.get(in)
    if view.isEmpty then return null
    return view.get
  }
  private def initTable(targetName: String, df: DataFrameTrait): Boolean = {
    df match
      case x: DataFramePartition => views += (targetName -> df)
      case _ => df.getDataFrame.createOrReplaceTempView(targetName)
    return true
  }

  def saveDf(targetName: String, df: List[DataFrameTrait]): Boolean = {
    df.length match
      case 1 => df.head.getPartitionName match
        case "" => {
          initTable(targetName, df.head)
          return true
        }
        case null => {
          initTable(targetName, df.head)
        }
        case _ => {
          initTable(s"${targetName}__${df.head.getPartitionName}", df.head)
          return true
        }
      case _ => {
        df.foreach(i => {
          initTable(s"${targetName}__${i.getPartitionName}", i)
        })
        return true
      }
  }
}