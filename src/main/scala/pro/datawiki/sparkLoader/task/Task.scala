package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameConsumer, DataFramePartition, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.configuration.ProgressStatus.done
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

trait Task {
  def run(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): ProgressStatus

  def setSkipIfEmpty(in: Boolean): Unit

  def setCache(in: TransformationCacheTrait): Unit
}

object Task {
  private var views: mutable.Map[String, DataFrameTrait] = mutable.Map()
  private var waiters: mutable.Map[String, DataFrameTrait] = mutable.Map()
  
  def getView(in: String): DataFrameTrait = {
    val view: Option[DataFrameTrait] = views.get(in)
    if view.isEmpty then return null
    return view.get
  }

  private def initTable(targetName: String, df: DataFrameTrait): ProgressStatus = {
    df match
      case x: DataFramePartition => views += (targetName -> df)
      case x: DataFrameConsumer => waiters += (targetName -> df)
      case _ => df.getDataFrame.createOrReplaceTempView(targetName)
    return ProgressStatus.done
  }

  def saveDf(targetName: String, df: DataFrameTrait): ProgressStatus = {
    if df.isEmpty then return {
      if !waiters.contains(targetName) then waiters += (targetName -> df)
      ProgressStatus.skip
    }
    saveDf(targetName = targetName, df = List.apply(df))
  }

  def isDefinedTable(targetName:String): Boolean = {
    if waiters.contains(targetName) then {
      return false
    }
    return true
  }

  def saveDf(targetName: String, df: List[DataFrameTrait]): ProgressStatus = {
    if waiters.contains(targetName) then {
      waiters -= targetName
    }
    df.length match
      case 1 => df.head.getPartitionName match
        case "" => {
          return initTable(targetName, df.head)
        }
        case null => {
          return initTable(targetName, df.head)
        }
        case _ => {
          return initTable(s"${targetName}__${df.head.getPartitionName}", df.head)
        }
      case _ => {
        df.foreach(i => {
          initTable(s"${targetName}__${i.getPartitionName}", i)
        })

        return done
      }
  }
}