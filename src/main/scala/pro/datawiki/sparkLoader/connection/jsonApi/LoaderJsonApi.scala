package pro.datawiki.sparkLoader.connection.jsonApi

import org.apache.hadoop.classification.InterfaceAudience.Private
import org.apache.spark.sql.functions.{lit, lit as row}
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.connection.Connection.connections
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.sparkLoader.{LogMode, YamlClass}
import sttp.client4.*

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class LoaderJsonApi(in: YamlConfig) extends ConnectionTrait {
  private var cache: TransformationCache = null

  @Private
  def localCache: TransformationCache = {
    if cache == null then cache = TransformationCache()
    return cache
  }

  def getValue(in: String, row: mutable.Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      result = result.replace(s"""${i._1}""", i._2)
    })

    return result
  }

  def run(row: Row): (DataFrame, String) = {
    var connections: mutable.Map[String, String] = mutable.Map()
    val currentDateTime: LocalDateTime = LocalDateTime.now()
    val nextDateTime: LocalDateTime = LocalDateTime.now().plusDays(1)
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    connections += (s"$${current_date}", currentDateTime.format(formatter))
    connections += (s"$${next_date}", nextDateTime.format(formatter))

    if row != null then {
      row.schema.fields.foreach(j => connections += (s"$${${j.name}}", row.get(row.fieldIndex(j.name)).toString))
    }

    val backend = DefaultSyncBackend()
    var request = basicRequest
    in.cookies.foreach(i => {
      request = request.cookie(i.key, connections(i.value))
    })
    val response = request.get(uri = uri"${getValue(in.getUrl, connections)}").send(backend)

    val resTxt = response.body match {
      case Left(e) => throw Exception(s"Got response exception:\n$e")
      case Right(r) => r
    }
    val partition = in.getSchemaByDataFrame(resTxt)
    localCache.saveRaw(resTxt)
    var df: DataFrame = localCache.readTable
    if row != null then {
      row.schema.fields.foreach(j => df = df.withColumn(j.name, lit(connections(s"$${${j.name}}"))))
    }
    df = df.withColumn("run_id", lit(RunConfig.getPartition))

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return (df, partition)
  }
}


object LoaderJsonApi extends YamlClass {
  def apply(inConfig: String): LoaderJsonApi = {
    val loader = new LoaderJsonApi(mapper.readValue(getLines(inConfig), classOf[YamlConfig]))

    return loader
  }
}
