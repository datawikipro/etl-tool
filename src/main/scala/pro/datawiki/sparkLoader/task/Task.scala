package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameConsumer, DataFramePartition, DataFrameTrait}
import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus.done
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

trait Task extends LoggingTrait {
  def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus
  def setSkipIfEmpty(in: Boolean): Unit
}