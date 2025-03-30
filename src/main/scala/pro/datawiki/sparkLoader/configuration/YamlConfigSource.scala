package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.SegmentationEnum.{full, random}
import pro.datawiki.sparkLoader.configuration.parent.LogicClass
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.{YamlConfigSourceAdHoc, YamlConfigSourceApiTrait, YamlConfigSourceDBSQL, YamlConfigSourceDBTable, YamlConfigSourceFileSystem, YamlConfigSourceKafkaTopic, YamlConfigSourceTrait, YamlConfigSourceWeb}
import pro.datawiki.sparkLoader.transformation.{TransformationCacheFileStorage, TransformationCacheNone, TransformationCacheTrait}

import scala.util.{Failure, Success}
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*


class YamlConfigSource(sourceName: String,
                       objectName: String,
                       segmentation: String,
                       sourceDb: YamlConfigSourceDBTable,
                       sourceSQL: YamlConfigSourceDBSQL,
                       sourceFileSystem: YamlConfigSourceFileSystem,
                       sourceKafkaTopic: YamlConfigSourceKafkaTopic,
                       sourceWeb: YamlConfigSourceWeb,
                       adHoc: YamlConfigSourceAdHoc,
                       cache: String,
                       asyncNumber: Int=64
                      
                      ) extends LogicClass {

  def init(): Unit = {
    reset()
    setLogic(sourceDb)
    setLogic(sourceSQL)
    setLogic(sourceFileSystem)
    setLogic(sourceKafkaTopic)
    setLogic(sourceWeb)
  }

  def getObjectName: String = objectName

  def getSegmentation: SegmentationEnum = {
    if isAdHoc then {
      return SegmentationEnum.adHoc
    }
    if segmentation == null then {
      return SegmentationEnum.full
    }
    segmentation match
      case "random" => return SegmentationEnum.random
      case "full" => return SegmentationEnum.full
      case "partition" => return SegmentationEnum.partition
      case _ => throw Exception()
  }

  def getSegments: List[String] = {
    return getSource.getSegments(Connection.getConnection(sourceName))
  }

  def getSource: YamlConfigSourceTrait = {
    init()

    super.getLogic match
      case x: YamlConfigSourceTrait => return x
      case _ => throw Exception()
  }

  def isAdHoc: Boolean = {
    adHoc match
      case null => false
      case _ => true
  }

  def getAdhocRow: List[Row] = {
    return adHoc.getAdhocRow
  }

  private def runMethod(parameters: Row, cache: TransformationCacheTrait): DataFrameTrait = {
    getSource match
      case x: YamlConfigSourceApiTrait => return x.getDataFrameAdHoc(sourceName = sourceName, adHoc = parameters, cache = cache)
      case x: YamlConfigSourceTrait => return x.getDataFrame(sourceName = sourceName, cache = cache)
      case _ => throw Exception()
  }

  private def threadLogic(in: Row, cacheConnection: ConnectionTrait, masterCache: TransformationCacheTrait): Boolean = {
    val treadCache = cacheConnection match
      case x: FileStorageTrait => TransformationCacheFileStorage(x)
      case _ => throw Exception()
    val treadDf = runMethod(in, treadCache)
    treadCache.saveTable(treadDf)
    masterCache.append(treadCache)
  }

  
  private def runAsync(): List[DataFrameTrait] = {
    val cacheConnection = Connection.getConnection(cache)
    val masterCache = cacheConnection match
      case x: FileStorageTrait => TransformationCacheFileStorage(x)
      case _ => throw Exception()
    val rows = getAdhocRow
    if rows.isEmpty then throw Exception()
    var skipped: Int = 0
    
    if LogMode.isDebug then {
      rows.foreach(element => threadLogic(element, cacheConnection, masterCache))
    } else {
      // Количество потоков
      var numThreads = asyncNumber
      if rows.length < numThreads then numThreads = rows.length

      // Создаем ExecutorService с фиксированным пулом из 8 потоков
      val executorService = Executors.newFixedThreadPool(numThreads)

      // Создаем ExecutionContext на основе ExecutorService
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

      // Создаем Future для каждого элемента массива
      val futures = rows.map { element =>
        Future {
          threadLogic(element, cacheConnection, masterCache)
        }
      }
      // Ожидаем завершения всех Future и собираем результаты
      val resultFuture = Future.sequence(futures)

      // Обработка результатов
      resultFuture.onComplete {
        case Success(results) =>
        case Failure(e) => println(s"Произошла ошибка: ${e.getMessage}")
      }

      // Ожидаем завершения всех Future (для демонстрации)
      Await.ready(resultFuture, 1800.seconds)
      executorService.shutdown()
    }
    if skipped > 0 then println(s"skipped ${skipped}")
    return masterCache.readDirty()

  }


  def run(segmentName: String): Boolean = {
    val df: List[DataFrameTrait] = getSegmentation match
      case SegmentationEnum.full => List.apply(runMethod(null, null))
      case SegmentationEnum.adHoc => runAsync()
      case _ => throw Exception()
    
    df.length match
      case 1 => df.head.getPartitionName match
        case "" => {
          df.head.get.createOrReplaceTempView(s"${getObjectName}")
          return true
        }
        case null => {
          df.head.get.createOrReplaceTempView(s"${getObjectName}")
          return true
        }
        case _ => {
          df.head.get.createOrReplaceTempView(s"${getObjectName}__${df.head.getPartitionName}")
          return true
        }
      case _ => {
        df.foreach(i => {
          i.get.createOrReplaceTempView(s"${getObjectName}__${i.getPartitionName}")
        })
        return true
      }

  }

}