package pro.datawiki.sparkLoader.connection.jsonApi

import pro.datawiki.exception.{ConfigurationException, DataProcessingException}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.yamlConfiguration.YamlClass
import sttp.client4.*

import java.net.URLEncoder
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.concurrent.duration.{Duration, SECONDS}

case class LoaderJsonApi(in: YamlConfig, configLocation: String) extends ConnectionTrait {
  private val _configLocation: String = configLocation

  logInfo("Creating JSON API connection")
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def getValueUrl(in: String, row: Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      result = result.replace(s"""${i._1}""", URLEncoder.encode(i._2, "UTF-8"))
    })

    return result
  }

  def getValue(in: String, row: Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      result = result.replace(s"""${i._1}""", i._2)
    })

    return result
  }

  def getVariables(row: Map[String, String]): Map[String, String] = {
    var connections: mutable.Map[String, String] = mutable.Map()
    connections += (s"$${current_date}", LocalDateTime.now().format(formatter))
    connections += (s"$${next_date}", LocalDateTime.now().plusDays(1).format(formatter))
    connections += (s"$${limit}", in.limit.toString)
    connections += (s"$${startOffset}", in.startOffset.toString)
    if row != null then {
      row.foreach(j => connections += (s"$${${j._1}}", j._2))
    }
    return connections.toMap
  }

  def retrySend(request2: Request[Either[String, String]], retryNumber: Int, url: String): String = {
    val backend = DefaultSyncBackend()

    try {
      val response = request2.send(backend)
      response.body match {
        case Left(e) => {
          println(request2)
          throw DataProcessingException(s"Ошибка при получении ответа от API: $e. body: ${response.body.toString}")
        }
        case Right(r) => return r
      }

    } catch {
      case e: Exception => {
        if retryNumber >= 4 then {
          return
            s"""{
               | "error": "Could not get data from remote server",
               | "url": "$url",
               | "error": ${e.toString}
               |}""".stripMargin
        }
        Thread.sleep(1000)
        return retrySend(request2, retryNumber + 1, url)
      }
    }

  }

  def sendRequest(variables: Map[String, String]): String = {

    var request = basicRequest

    // Добавляем кастомные заголовки из конфигурации
    in.headers.foreach(h => {
      variables.get(h.value) match {
        case Some(value) => request = request.header(h.key, value)
        case None => request = request.header(h.key, h.value)
      }
    })

    // Добавляем стандартные заголовки браузера, если они не переопределены в конфигурации
    if (!in.headers.exists(_.key.toLowerCase == "user-agent")) {
      request = request.header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    }
    if (!in.headers.exists(_.key.toLowerCase == "accept")) {
      request = request.header("Accept", "application/json, text/plain, */*")
    }
    if (!in.headers.exists(_.key.toLowerCase == "accept-language")) {
      request = request.header("Accept-Language", "ru-RU,ru;q=0.9,en;q=0.8")
    }
    if (!in.headers.exists(_.key.toLowerCase == "referer")) {
      request = request.header("Referer", "https://kino.kz/")
    }

    //    in.cookies.foreach(i => request = request.cookie(i.key, variables(i.value)))

    if in.authType != null then {
      if in.authType.BearerToken != null then request = request.auth.bearer(in.authType.BearerToken)
    }
    request = request.readTimeout(Duration(in.timeoutSeconds, SECONDS))


    in.method match {
      case "Get" => {
        request = request.contentType("application/json")
        val request2: Request[Either[String, String]] = request.get(uri = uri"${getValueUrl(in.url, variables)}")
        return retrySend(request2, 0, getValueUrl(in.url, variables))
      }
      case "Post" => {
        if in.formData.nonEmpty then {
          val formMap = in.formData.map(i => i.key -> getValue(i.value, variables)).toMap
          request = request.body(formMap)
        }

        if in.body != "" then {
          request = request.contentType("application/json")
          request = request.body(getValue(in.body, variables)
          )
        }
        val request2: Request[Either[String, String]] = request.post(uri = uri"${getValueUrl(in.url, variables)}")
        return retrySend(request2, 0, getValueUrl(in.url, variables))
      }
      case _ =>
        throw ConfigurationException("Метод в LoaderJsonApi еще не реализован")
    }

  }

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.jsonApi
  }

  override def getConfigLocation(): String = {
    _configLocation
  }
}


object LoaderJsonApi extends YamlClass {
  def apply(inConfig: String): LoaderJsonApi = {
    val loader = new LoaderJsonApi(mapper.readValue(getLines(inConfig), classOf[YamlConfig]), inConfig)

    return loader
  }
}
