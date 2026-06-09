package pro.datawiki.sparkLoader.context

import pro.datawiki.datawarehouse.{DataFrameBaseSchema, DataFrameConsumer, DataFramePartition, DataFrameTrait}
import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

object SparkContext extends LoggingTrait {
  private var views: mutable.Map[String, DataFrameTrait] = mutable.Map()
  private var waiters: mutable.Map[String, DataFrameTrait] = mutable.Map()
  private var tables: mutable.Map[String, DataFrameTrait] = mutable.Map()

  def initTables(): Unit = {
    tables.map(col => {
      col._2.getDataFrame.createOrReplaceTempView(col._1)
    })
  }

  def getView(in: String): DataFrameTrait = {
    val view: Option[DataFrameTrait] = views.get(in)
    if view.nonEmpty then return view.get

    val table: Option[DataFrameTrait] = tables.get(in)
    if table.nonEmpty then return table.get

    return null
  }

  private def initTable(targetName: String, df: DataFrameTrait): ProgressStatus = {
    try {
      df match
        case x: DataFramePartition => views += (targetName -> df)
        case x: DataFrameConsumer => waiters += (targetName -> df)
        case x: DataFrameBaseSchema => tables += (targetName -> df)
        case _ => df.getDataFrame.createOrReplaceTempView(targetName)
    } catch {
      case e: Exception =>
        logError("init table", e, s"target: $targetName")
        throw DataProcessingException(s"Failed to initialize table: $targetName", e)
    }
    return ProgressStatus.done
  }

  def saveDf(targetName: String, df: DataFrameTrait): ProgressStatus = {
    if df.isEmpty then {
      if !waiters.contains(targetName) then waiters += (targetName -> df)
      return ProgressStatus.skip
    }
    logInfo(s"Saving single DataFrame for target: $targetName")
    saveDf(targetName = targetName, df = List.apply(df))
  }

  def isDefinedTable(targetName: String): Boolean = {
    val isDefined = !waiters.contains(targetName)
    logDebug(s"Table $targetName is defined: $isDefined")
    return isDefined
  }

  def saveDf(targetName: String, df: List[DataFrameTrait]): ProgressStatus = {
    if waiters.contains(targetName) then waiters -= targetName

    df.foreach(col => initTable(col.getFullName(targetName), col))

    return ProgressStatus.done
  }
}
