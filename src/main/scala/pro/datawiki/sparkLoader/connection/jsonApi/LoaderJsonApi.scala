package pro.datawiki.sparkLoader.connection.jsonApi

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.transformation.{TransformationCacheFileStorage, TransformationCacheTrait}
import pro.datawiki.sparkLoader.{LogMode, YamlClass}
import sttp.client4.*

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class LoaderJsonApi(in: YamlConfig) extends ConnectionTrait {
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private var retryCounter = 0

  def getValue(in: String, row: mutable.Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      result = result.replace(s"""${i._1}""", i._2)
    })

    return result
  }

  def getVariables(row: Row): mutable.Map[String, String] = {
    var connections: mutable.Map[String, String] = mutable.Map()

    connections += (s"$${current_date}", LocalDateTime.now().format(formatter))
    connections += (s"$${next_date}", LocalDateTime.now().plusDays(1).format(formatter))
    connections += (s"$${limit}", in.limit.toString)
    connections += (s"$${startOffset}", in.startOffset.toString)
    if row != null then {
      row.schema.fields.foreach(j => connections += (s"$${${j.name}}", row.get(row.fieldIndex(j.name)).toString))
    }
    return connections
  }

  def sendRequest(connections: mutable.Map[String, String]): String = {
    val backend = DefaultSyncBackend()
    var request = basicRequest
    in.cookies.foreach(i => {
      request = request.cookie(i.key, connections(i.value))
    })

    if in.authType != null then {
      if in.authType.BearerToken != null then request.auth.bearer(in.authType.BearerToken)
    }

    val response = request.get(uri = uri"${getValue(in.getUrl, connections)}").send(backend)

    val resTxt = response.body match {
      case Left(e) => throw Exception(s"Got response exception:\n$e")
      case Right(r) => r
    }
    return resTxt
  }

  def getDataFrameOverCache(json: String, cache: TransformationCacheTrait): DataFrame = {
    val localCache: TransformationCacheFileStorage = cache match
      case x: TransformationCacheFileStorage => x
      case _ => throw Exception()
    localCache.saveRaw(json)
    return localCache.readBaseTable()
  }

  def getDataFrameFromJson(json: String, cache: TransformationCacheTrait): DataFrameTrait = {
    if in.isValidationScript then {
      val df = in.getSchemaByJson(json)
      if df == null then
        return DataFrameDirty("error", getDataFrameOverCache(json, cache), false)

      LogMode.debugDF(df.get)
      return df
    } else {
      return DataFrameOriginal(getDataFrameOverCache(json, cache))
    }

  }

  def run(row: Row, cache: TransformationCacheTrait): DataFrameTrait = {

    var connections: mutable.Map[String, String] = getVariables(row)

    val resTxt = sendRequest(connections)

    var df = getDataFrameFromJson(resTxt, cache)

    if row != null then {
      row.schema.fields.foreach(j => df.addColumn(j.name, lit(connections(s"$${${j.name}}"))))
    }
    df.addColumn("run_id", lit(RunConfig.getPartition))
    LogMode.debugDF(df.get)

    if df.isValidData then
      return df

    retryCounter += 1
    if retryCounter > 4 then
      return df

    Thread.sleep(5000)
    return run(row = row, cache = cache)
  }
}


object LoaderJsonApi extends YamlClass {
  def apply(inConfig: String): LoaderJsonApi = {
    val loader = new LoaderJsonApi(mapper.readValue(getLines(inConfig), classOf[YamlConfig]))

    return loader
  }
}
