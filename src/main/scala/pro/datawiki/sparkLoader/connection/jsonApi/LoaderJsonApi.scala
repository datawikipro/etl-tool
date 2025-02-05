package pro.datawiki.sparkLoader.connection.jsonApi

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}
import sttp.client4.*
import org.apache.spark.sql.functions.lit

class LoaderJsonApi(in: YamlConfig) extends ConnectionTrait {
  private val localCache: TransformationCache = new TransformationCache(LoaderLocalJson("config/unico/connection/local.yaml"))

  def getValue(in:String, row: Row):String ={
    var result: String = in
    if row != null then {
      row.schema.fields.foreach(j => 
        result = result.replace("""${""" + j.name + """}""", row.get(row.fieldIndex(j.name)).toString))
    }
    return result
  }
  
  def run(row: Row): DataFrame = {
    val backend = DefaultSyncBackend()
    val request = basicRequest
    in.cookies.foreach(i=> {
      request.cookie(i.key,getValue(i.value,row))
    })
    val response = request.get(uri = uri"${getValue(in.getUrl,row)}").send(backend)

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
