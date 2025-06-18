package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

case class TaskTemplateIdMapConfig(systemCode: String,
                                   columnNames: List[String],
                                   domainName: String)
