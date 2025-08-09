package pro.datawiki.sparkLoader.connection.jsonApi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.lit
import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.{ConfigurationException, DataProcessingException, SchemaValidationException}
import pro.datawiki.schemaValidator.{DataFrameConstructor, SchemaValidator}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.yamlConfiguration.YamlClass
import sttp.client4.*

import java.net.URLEncoder
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.concurrent.duration.{Duration, MINUTES}

class LoaderJsonApi(in: YamlConfig) extends ConnectionTrait {
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private var retryCounter = 0


  def getValueUrl(in: String, row: mutable.Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      result = result.replace(s"""${i._1}""", URLEncoder.encode(i._2, "UTF-8"))
    })

    return result
  }

  def getValue(in: String, row: mutable.Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      result = result.replace(s"""${i._1}""", i._2)
    })

    return result
  }

  def getVariables(row: mutable.Map[String, String]): mutable.Map[String, String] = {
    var connections: mutable.Map[String, String] = mutable.Map()
    connections += (s"$${current_date}", LocalDateTime.now().format(formatter))
    connections += (s"$${next_date}", LocalDateTime.now().plusDays(1).format(formatter))
    connections += (s"$${limit}", in.limit.toString)
    connections += (s"$${startOffset}", in.startOffset.toString)
    if row != null then {
      row.foreach(j => connections += (s"$${${j._1}}", j._2))
    }
    return connections
  }

  def sendRequest(variables: mutable.Map[String, String]): String = {
    val backend = DefaultSyncBackend()
    var request = basicRequest

    in.cookies.foreach(i => {
      request = request.cookie(i.key, variables(i.value))
    })

    if in.authType != null then {
      if in.authType.BearerToken != null then request = request.auth.bearer(in.authType.BearerToken)
    }
    request = request.readTimeout(Duration(2, MINUTES))
    request = request.contentType("application/json")
    //    if !(in.getHost == null) then
    //      request = request.header("Host", in.getHost)
    val response = in.method match {
      case "Get" => {
        val request2 = request.get(uri = uri"${getValueUrl(in.getUrl, variables)}")
        request2.send(backend)
      }
      case "Post" => {
        request = request.body({
          getValue(in.body, variables)
        })
        val request2 = request.post(uri = uri"${getValueUrl(in.getUrl, variables)}")
        request2.send(backend)
      }
      case _ =>
        throw new ConfigurationException("Метод в LoaderJsonApi еще не реализован")
    }

    val resTxt = response.body match {
      case Left(e) => throw new DataProcessingException(s"Ошибка при получении ответа от API: $e")
      case Right(r) => r
    }
    return resTxt
  }

  def getDataFrameOverCache(jsonString: String): DataFrame = {
    return DataFrameConstructor.getDataFrameFromJsonWithOutTemplate(jsonString)


  }

  def getDataFrameFromJson(json: String): DataFrameTrait = {
    try {
      if (json == null || json.isEmpty) {
        throw new DataProcessingException("Пустая или null JSON строка передана в getDataFrameFromJson")
      }

      if in.isValidationScript then {
        val df = in.getSchemaByJson(json)
        if df == null then
          return DataFrameDirty("error", getDataFrameOverCache(json), false)

      LogMode.debugDF(df.getDataFrame)
      return df
    } else {
      return DataFrameOriginal(getDataFrameOverCache(json))
    }
    } catch {
      case e: SchemaValidationException => throw e // пробрасываем исключения валидации схемы
      case e: DataProcessingException => throw e // пробрасываем исключения обработки данных
      case e: Exception => throw new DataProcessingException(s"Ошибка при получении DataFrame из JSON: ${e.getMessage}", e)
    }
  }

  def run(row: mutable.Map[String, String]): DataFrameTrait = {

    var variablesList: mutable.Map[String, String] = getVariables(row)

    val resTxt = sendRequest(variablesList)
    var df: DataFrameTrait = in.resultType match {
      case "json" => {
        getDataFrameFromJson(resTxt)
      }

      case _ => {


        val lines: Array[String] = resTxt.split("\n")
//        val ds: Dataset[String] = SparkObject.spark.createDataset(lines)
//         Читаем RDD как DataFrame
//        val df:DataFrame = SparkObject.spark.read
//          .option("header", "true") // используем первую строку как заголовок
//          .option("delimiter", ";") // задаём разделитель
//          .option("inferSchema", "true") // автоматическое определение типов данных
//          .csv(ds)
//
//         Проверяем результат
//        df.show()
//        df.printSchema()
        throw Exception()
      }
    }


    if row != null then {
      row.foreach(j => df.addColumn(j._1, lit(variablesList(s"$${${j._1}}"))))
    }
    df.addColumn("run_id", lit(RunConfig.getPartition))
    LogMode.debugDF(df.getDataFrame)

    if df.isValidData then
      return df

    retryCounter += 1
    if retryCounter > 4 then
      return df

    Thread.sleep(5000)
    return run(row = row)
  }

  override def close(): Unit = {}
}


object LoaderJsonApi extends YamlClass {
  def apply(inConfig: String): LoaderJsonApi = {
    val loader = new LoaderJsonApi(mapper.readValue(getLines(inConfig), classOf[YamlConfig]))

    return loader
  }
}
