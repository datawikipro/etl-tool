package pro.datawiki.sparkLoader.connection.qdrant

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, NoSQLDatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass
import sttp.client4.*

import scala.collection.mutable.ListBuffer

class LoaderQdrant(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait, NoSQLDatabaseTrait, LoggingTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating Qdrant connection")

  private var server: YamlServerHost = null
  private implicit val backend: SyncBackend = DefaultSyncBackend()

  override def readDf(location: String): DataFrame = {
    try {
      if (server == null) {
        server = getServer
      }

      // Получаем все точки из коллекции через HTTP API
      val response = getCollectionPoints(location)

      // Конвертируем результаты в DataFrame
      val rows = convertResponseToRows(response)
      val schema = createSchema()

      val rowsRDD = SparkObject.spark.sparkContext.parallelize(rows)
      val df = SparkObject.spark.createDataFrame(rowsRDD, schema)

      LogMode.debugDF(df)
      return df
    } catch {
      case e: Exception =>
        logger.error(s"Error reading Qdrant collection: $location", e)
        throw e
    }
  }

  override def writeDf(df: DataFrame, tableFullName: String, writeMode: WriteMode): Unit = {
    try {
      if (server == null) {
        server = getServer
      }

      // Создаем коллекцию если не существует
      createCollectionIfNotExists(tableFullName)

      // Конвертируем DataFrame в точки Qdrant
      val points = convertDataFrameToPoints(df, tableFullName)

      // Записываем точки в коллекцию
      upsertPoints(tableFullName, points)

      logger.info(s"Successfully wrote ${points.size} points to collection: $tableFullName")
    } catch {
      case e: Exception =>
        logger.error(s"Error writing to Qdrant collection: $tableFullName", e)
        throw e
    }
  }
//
//  override def readDfSchema(location: String): DataFrame = {
//    // Для Qdrant возвращаем пустой DataFrame с базовой схемой
//    val schema = createSchema()
//    val emptyRDD = SparkObject.spark.sparkContext.emptyRDD[Row]
//    SparkObject.spark.createDataFrame(emptyRDD, schema)
//  }

  private def getServer: YamlServerHost = {
    configYaml.server.replica.foreach { replica =>
      if (replica.validateHost) {
        return replica
      }
    }
    if (configYaml.server.master.validateHost) {
      return configYaml.server.master
    }
    throw new Exception("No valid Qdrant server found")
  }

  private def getBaseUrl: String = {
    s"http://${server.host}:${server.httpPort}"
  }

  private def getCollectionPoints(collectionName: String): String = {
    val url = s"${getBaseUrl}/collections/$collectionName/points/scroll"
    val request = basicRequest
      .post(uri"$url")
      .header("Content-Type", "application/json")
      .body("""{"limit": 10000}""")

    val response = request.send(backend)
    response.body match {
      case Right(body) => body
      case Left(error) => throw new Exception(s"HTTP error: $error")
    }
  }

  private def createCollectionIfNotExists(collectionName: String): Unit = {
    try {
      val url = s"${getBaseUrl}/collections/$collectionName"
      val request = basicRequest
        .get(uri"$url")
        .header("Content-Type", "application/json")

      val response = request.send(backend)

      // Если коллекция не существует, создаем её
      if (response.code.code == 404) {
        createCollection(collectionName)
      }
    } catch {
      case e: Exception =>
        logger.debug(s"Collection $collectionName might already exist: ${e.getMessage}")
    }
  }

  private def createCollection(collectionName: String): Unit = {
    val url = s"${getBaseUrl}/collections/$collectionName"
    val requestBody =
      """{
      "vectors": {
        "size": 384,
        "distance": "Cosine"
      }
    }"""

    val request = basicRequest
      .put(uri"$url")
      .header("Content-Type", "application/json")
      .body(requestBody)

    val response = request.send(backend)
    response.body match {
      case Right(_) => logger.info(s"Created collection: $collectionName")
      case Left(error) => throw new Exception(s"Failed to create collection: $error")
    }
  }

  private def upsertPoints(collectionName: String, points: List[QdrantPoint]): Unit = {
    val url = s"${getBaseUrl}/collections/$collectionName/points"
    val requestBody =
      s"""{
      "points": ${points.map(_.toJson).mkString("[", ",", "]")}
    }"""

    val request = basicRequest
      .put(uri"$url")
      .header("Content-Type", "application/json")
      .body(requestBody)

    val response = request.send(backend)
    response.body match {
      case Right(_) => logger.info(s"Upserted ${points.size} points to collection: $collectionName")
      case Left(error) => throw new Exception(s"Failed to upsert points: $error")
    }
  }

  private def convertResponseToRows(response: String): List[Row] = {
    val rows = ListBuffer[Row]()

    // Простой парсинг JSON без внешних библиотек
    try {
      val pointsStart = response.indexOf("\"points\":[")
      if (pointsStart != -1) {
        val pointsEnd = response.indexOf("]", pointsStart)
        if (pointsEnd != -1) {
          val pointsJson = response.substring(pointsStart + 9, pointsEnd + 1)

          // Простой парсинг точек
          var currentPos = 0
          while (currentPos < pointsJson.length) {
            val pointStart = pointsJson.indexOf("{", currentPos)
            if (pointStart == -1) return rows.toList

            val pointEnd = findMatchingBrace(pointsJson, pointStart)
            if (pointEnd == -1) return rows.toList

            val pointJson = pointsJson.substring(pointStart, pointEnd + 1)
            val point = parsePoint(pointJson)
            if (point.isDefined) {
              rows += Row(point.get.id, point.get.vector, point.get.payload)
            }

            currentPos = pointEnd + 1
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse response: ${e.getMessage}")
    }

    rows.toList
  }

  private def findMatchingBrace(json: String, start: Int): Int = {
    var braceCount = 0
    var i = start
    while (i < json.length) {
      json.charAt(i) match {
        case '{' => braceCount += 1
        case '}' =>
          braceCount -= 1
          if (braceCount == 0) return i
        case _ => // do nothing
      }
      i += 1
    }
    -1
  }

  private def parsePoint(pointJson: String): Option[QdrantPoint] = {
    try {
      val id = extractStringValue(pointJson, "id").getOrElse("0")
      val vector = extractArrayValue(pointJson, "vector").getOrElse(List.empty[Double])
      val payload = extractStringValue(pointJson, "payload").getOrElse("{}")

      Some(QdrantPoint(id, vector, Map("raw" -> payload)))
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse point: ${e.getMessage}")
        None
    }
  }

  private def extractStringValue(json: String, key: String): Option[String] = {
    val pattern = s""""$key":\\s*"([^"]*)" """.r
    pattern.findFirstMatchIn(json).map(_.group(1))
  }

  private def extractArrayValue(json: String, key: String): Option[List[Double]] = {
    val pattern = s""""$key":\\s*\\[([^\\]]*)\\] """.r
    pattern.findFirstMatchIn(json).map { matchResult =>
      val arrayContent = matchResult.group(1)
      arrayContent.split(",").map(_.trim.toDouble).toList
    }
  }

  private def convertDataFrameToPoints(df: DataFrame, collectionName: String): List[QdrantPoint] = {
    val points = ListBuffer[QdrantPoint]()
    var pointId = 1L

    df.collect().foreach { row =>
      // Ищем колонку с векторами
      val vectorColumn = df.columns.find(col =>
        col.toLowerCase.contains("vector") ||
          col.toLowerCase.contains("embedding") ||
          col.toLowerCase.contains("vec")
      )

      val vector = if (vectorColumn.isDefined) {
        try {
          row.getAs[Seq[Double]](vectorColumn.get).toList
        } catch {
          case _: Exception => List.empty[Double]
        }
      } else {
        List.empty[Double]
      }

      // Создаем payload из остальных полей
      val payload = scala.collection.mutable.Map[String, Any]()
      df.columns.foreach { colName =>
        if (colName != vectorColumn.getOrElse("")) {
          val value = row.getAs[Any](colName)
          payload.put(colName, value)
        }
      }

      points += QdrantPoint(
        id = pointId.toString,
        vector = vector,
        payload = payload.toMap
      )
      pointId += 1
    }

    points.toList
  }

  private def createSchema(): StructType = {
    StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("vector", StringType, nullable = true),
      StructField("payload", StringType, nullable = true)
    ))
  }

  override def close(): Unit = {
    // HTTP клиент не требует явного закрытия
    logger.info("Qdrant HTTP client closed")
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.qdrant
  }

  override def getConfigLocation(): String = {
    _configLocation
  }
}

object LoaderQdrant extends YamlClass {
  def apply(inConfig: String): LoaderQdrant = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    new LoaderQdrant(configYaml, inConfig)
  }
}