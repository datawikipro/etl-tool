package pro.datawiki.sparkLoader

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.EltConfig
import pro.datawiki.sparkLoader.connection.kafka.LoaderKafka
import pro.datawiki.sparkLoader.connection.kafkaMSK.LoaderKafkaMSK
import pro.datawiki.sparkLoader.connection.minIo.LoaderMinIo
import pro.datawiki.sparkLoader.connection.mysql.LoaderMySql
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.connection.s3.LoaderS3
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, QueryTrait}
import pro.datawiki.sparkLoader.transformation.TransformationIdMap

import scala.collection.mutable

private var collections: mutable.Map[String, ConnectionTrait] = mutable.Map()
private var target: ConnectionTrait = null

@main
def sparkRun(configLocation: String, partitionKey: String = "", partitionId: String=""): Unit = {

  val etlConfig = EltConfig.apply(configLocation)

  etlConfig.connections.foreach(i => {
    i.connection match
      case "mysql" => collections += (i.sourceName -> LoaderMySql(i.configLocation))
      case "postgres" => collections += (i.sourceName -> LoaderPostgres(i.configLocation))
      case "kafka" => collections += (i.sourceName -> LoaderKafka(i.configLocation))
      case "kafkaAmazon" => collections += (i.sourceName -> LoaderKafkaMSK(i.configLocation))
      case "s3Amazon" => collections += (i.sourceName -> LoaderS3(i.configLocation))
      case _ =>
        throw Exception()
  })

  if etlConfig.idmap.sourceName != null then {
    collections(etlConfig.idmap.sourceName) match
      case x: DatabaseTrait => TransformationIdMap.setIdmap(x)
      case x: QueryTrait => throw Exception()
      case _ => throw Exception()
  }

  etlConfig.source.foreach(i => {
    val src = collections(i.sourceName)
    var df: DataFrame = null
    src match
      case x: DatabaseTrait => {
        if partitionKey != "" then {
          df = x.getDataFrameBySQL(s"select ${i.sourceDb.getColumnNames.mkString(",")} from ${i.sourceDb.tableSchema}.${i.sourceDb.tableName} where $partitionKey = $partitionId")   
        } else {
          df = x.getDataFrameBySQL(s"select ${i.sourceDb.getColumnNames.mkString(",")} from ${i.sourceDb.tableSchema}.${i.sourceDb.tableName}")
        }
        
        df.show()
        df.printSchema()
        df.createTempView(i.objectName)
      }
      case x: QueryTrait => {
        df = x.getDataFrameFromTopic("NewTopic")
        df.show()
        df.printSchema()
        df.createTempView(i.objectName)
      }
      case _ => throw Exception()
  })

  etlConfig.transformations.foreach(i => {
    var df: DataFrame = SparkObject.spark.sql(s"select * from ${i.idmap.sourceName}")

    i.idmap.idmaps.foreach(j => {
      val idmap = TransformationIdMap(i.idmap.sourceName, i.idmap.tenantName, j.domainName, j.isGenerated, j.columnNames)
      df = idmap.addendNewKeys(df)
      df.printSchema()
      df.show()
    })
    df.createTempView(i.objectName)
  })

  target = collections(etlConfig.target.connection)

  val df = SparkObject.spark.sql("select * from target")
  df.show()
  df.printSchema()
  target.writeDf(etlConfig.target.targetFile, df)
}