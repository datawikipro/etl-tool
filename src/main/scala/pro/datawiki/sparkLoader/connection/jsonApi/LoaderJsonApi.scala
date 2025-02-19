package pro.datawiki.sparkLoader.connection.jsonApi

import org.apache.hadoop.classification.InterfaceAudience.Private
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.sparkLoader.{LogMode, YamlClass}
import sttp.client4.*

class LoaderJsonApi(in: YamlConfig) extends ConnectionTrait {
  private var cache: TransformationCache = null

  @Private
  def localCache: TransformationCache = {
    if cache == null then cache = TransformationCache()
    return cache
  }

  def getValue(in: String, row: Row): String = {
    var result: String = in
    if row != null then {
      row.schema.fields.foreach(j =>
        result = result.replace(s"""$${${j.name}}""", row.get(row.fieldIndex(j.name)).toString))
    }
    return result
  }

  def run(row: Row): DataFrame = {
    val backend = DefaultSyncBackend()
    var request = basicRequest
    in.cookies.foreach(i => {
      request = request.cookie(i.key, getValue(i.value, row))
    })
    val response = request.get(uri = uri"${getValue(in.getUrl, row)}").send(backend)

    val resTxt = response.body match {
      case Left(e) => throw Exception(s"Got response exception:\n$e")
      case Right(r) => r
    }
    localCache.saveRaw(resTxt)
    var df: DataFrame = localCache.readTable
    if row != null then {
      row.schema.fields.foreach(j => df = df.withColumn(j.name, lit(row.get(row.fieldIndex(j.name)).toString)))
    }

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }
}


object LoaderJsonApi extends YamlClass {
  def apply(inConfig: String): LoaderJsonApi = {
    val loader = new LoaderJsonApi(mapper.readValue(getLines(inConfig), classOf[YamlConfig]))

    return loader
  }
}
