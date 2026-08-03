package pro.datawiki.sparkLoader.partition

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import pro.datawiki.sparkLoader.configuration.YamlConfigPartitionSync
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.connection.minIo.minioIceberg.LoaderMinIoIceberg
import pro.datawiki.sparkLoader.dictionaryEnum.SCDType
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

object PartitionDeltaChecker extends LoggingTrait {

  def resolveCandidatePartitions(config: YamlConfigPartitionSync): List[String] = {
    if (config.partitionList.nonEmpty) {
      config.partitionList
    } else config.lookbackDays match {
      case Some(days) if days > 0 =>
        val today = LocalDate.now()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        (0 to days).map(d => today.minusDays(d).format(formatter)).toList
      case _ =>
        try {
          val p = ApplicationContext.getGlobalVariable("partition")
          if (p != null && p.nonEmpty) List(p) else List.empty
        } catch {
          case _: Exception => List.empty
        }
    }
  }

  def fetchMetricsFromSource(
                              srcConn: ConnectionTrait,
                              srcTableName: String,
                              srcWhere: String,
                              config: YamlConfigPartitionSync,
                              candidatePartitions: List[String]
                            ): Map[String, PartitionMetrics] = {
    val partCol = config.partitionColumn
    val partitionsFilter = if (candidatePartitions.nonEmpty) {
      s"$partCol IN (${candidatePartitions.map(p => s"'$p'").mkString(", ")})"
    } else "1=1"

    val srcWhereClean = if (srcWhere != null && srcWhere.trim.nonEmpty && srcWhere.trim.toLowerCase != "null") srcWhere.trim else null

    val baseFilter = if (srcWhereClean != null) {
      s"($srcWhereClean) AND ($partitionsFilter)"
    } else {
      partitionsFilter
    }

    val hashExpr = config.hashExpression.getOrElse("1")
    val query =
      s"""SELECT $partCol AS part_val, COUNT(1) AS row_cnt, COALESCE(CAST($hashExpr AS STRING), '') AS hash_val
         |FROM $srcTableName
         |WHERE $baseFilter
         |GROUP BY $partCol""".stripMargin

    executeQueryMetrics(srcConn, query, config.odsTable)
  }

  def fetchMetricsFromOds(
                           odsConn: ConnectionTrait,
                           config: YamlConfigPartitionSync,
                           candidatePartitions: List[String]
                         ): Map[String, PartitionMetrics] = {
    val partCol = config.partitionColumn
    val partitionsFilter = if (candidatePartitions.nonEmpty) {
      s"$partCol IN (${candidatePartitions.map(p => s"'$p'").mkString(", ")})"
    } else "1=1"

    val activeFilter = SCDType(config.scdType) match {
      case SCDType.SCD_2 =>
        config.scdActiveFilter match {
          case Some(filter) if filter.trim.nonEmpty => filter
          case _ => "valid_to_dttm = TIMESTAMP '9999-12-31 00:00:00'"
        }
      case _ => "1=1"
    }

    val whereClause = s"($activeFilter) AND ($partitionsFilter)"
    val hashExpr = config.hashExpression.getOrElse("1")
    val query =
      s"""SELECT $partCol AS part_val, COUNT(1) AS row_cnt, COALESCE(CAST($hashExpr AS STRING), '') AS hash_val
         |FROM ${config.odsTable}
         |WHERE $whereClause
         |GROUP BY $partCol""".stripMargin

    executeQueryMetrics(odsConn, query, config.odsTable)
  }

  private def executeQueryMetrics(conn: ConnectionTrait, sql: String, tableNameOpt: String = ""): Map[String, PartitionMetrics] = {
    val result = mutable.Map[String, PartitionMetrics]()
    conn match {
      case db: DatabaseTrait =>
        try {
          val df = db.getDataFrameBySQL(sql)
          df.collect().foreach { row =>
            val partVal = row.getAs[Any]("part_val").toString
            val rowCnt = row.getAs[Number]("row_cnt").longValue()
            val hashVal = Option(row.getAs[Any]("hash_val")).map(_.toString)
            result += (partVal -> PartitionMetrics(partVal, rowCnt, hashVal))
          }
        } catch {
          case e: Exception =>
            logError("partition metrics query execution", e, s"sql: $sql")
        }
      case iceberg: LoaderMinIoIceberg =>
        try {
          iceberg.modifySpark()
          val fullRef = if (tableNameOpt.nonEmpty) iceberg.fullRef(tableNameOpt) else sql
          val icebergSql = if (tableNameOpt.nonEmpty && sql.contains(tableNameOpt)) {
            sql.replace(tableNameOpt, fullRef)
          } else sql
          logInfo(s"PartitionDeltaChecker: Executing Iceberg Spark SQL: $icebergSql")
          val df = SparkObject.spark.sql(icebergSql)
          df.collect().foreach { row =>
            val partVal = row.getAs[Any]("part_val").toString
            val rowCnt = row.getAs[Number]("row_cnt").longValue()
            val hashVal = Option(row.getAs[Any]("hash_val")).map(_.toString)
            result += (partVal -> PartitionMetrics(partVal, rowCnt, hashVal))
          }
        } catch {
          case e: Exception =>
            logError("partition metrics query execution on Iceberg", e, s"sql: $sql")
        }
      case _ =>
        logWarning(s"PartitionDeltaChecker: connection ${conn.getClass.getSimpleName} does not support SQL metrics query directly.")
    }
    result.toMap
  }

  def getDirtyPartitions(
                          srcMetrics: Map[String, PartitionMetrics],
                          odsMetrics: Map[String, PartitionMetrics],
                          candidatePartitions: List[String]
                        ): List[String] = {
    candidatePartitions.filter { p =>
      val srcOpt = srcMetrics.get(p)
      val odsOpt = odsMetrics.get(p)

      (srcOpt, odsOpt) match {
        case (Some(srcM), Some(odsM)) =>
          if (srcM.count != odsM.count) true
          else if (srcM.hashValue != odsM.hashValue) true
          else false
        case (Some(_), None) => true // Missing in ODS
        case (None, Some(_)) => false // Missing in SRC
        case (None, None) => false
      }
    }
  }
}
