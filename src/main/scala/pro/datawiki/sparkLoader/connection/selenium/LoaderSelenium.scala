package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.{DataFrame, Row}
import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium.getWebDriver
import pro.datawiki.sparkLoader.transformation.{TransformationCacheFileStorage, TransformationCacheTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}
import org.apache.spark.sql.functions.{lit, lit as row}
import pro.datawiki.schemaValidator.{SparkRow, SparkRowAttribute}

import java.time.Duration


class LoaderSelenium(configYaml: YamlConfig) extends ConnectionTrait {
  private var cache: TransformationCacheFileStorage = null

  def set(inCache: String): Unit = {
    val con = Connection.getConnection(inCache)
    cache = con match
      case x: FileStorageTrait => TransformationCacheFileStorage(x)
      case _ => throw Exception()
  }

  private def localCache: TransformationCacheFileStorage = {
    if cache == null then throw Exception()
    return cache
  }

  private def getDataFrameFromJson(resTxt: String): DataFrame = {
    localCache.saveRaw(resTxt)
    var df: DataFrame = localCache.readBaseTable()
    return df
  }

  private def getDataFrameFromCustom(inSeleniumList: SeleniumList, inYamlConfig: YamlConfig): DataFrame = {

    if inYamlConfig == null then {
      throw Exception()
    }
    var listFieldsAttribute: List[SparkRowAttribute] = List.apply()

    inYamlConfig.getSchema.foreach(i => {
      listFieldsAttribute = listFieldsAttribute.appended(i.getStructField(inSeleniumList))
    })
    val sparkRow = SparkRow(listFieldsAttribute)

    return sparkRow.getDataFrame

  }

  def run(row: Row): DataFrameTrait = {
    var df: DataFrame = null
    val webDriver = getWebDriver
    val newConfigYaml = YamlConfig.apply(in = configYaml, row = row)
    webDriver.get(newConfigYaml.getUrl)
    webDriver.manage.timeouts.implicitlyWait(Duration.ofSeconds(5))

    val html: WebElement = webDriver.findElement(By.tagName("html"))
    val result: SeleniumList = SeleniumList.apply()

    newConfigYaml.getTemplate.foreach(i => result.appendElements(i.getSubElements(html)))

    if configYaml.getSchema.length == 1 then {
      if configYaml.getSchema.head.getType == "json" then {
        result.getList.head.value.getValue match
          case x: String => df = getDataFrameFromJson(x)
          case _ => throw Exception()

      } else {
        df  = getDataFrameFromCustom(result,newConfigYaml)
      }
    } else {
      df  = getDataFrameFromCustom(result,newConfigYaml)
    }

    LogMode.debugDF(df)
    return DataFrameOriginal(df)
  }

  override def close(): Unit = {
    LoaderSelenium.close()
  }
}

object LoaderSelenium extends YamlClass {
  def apply(inConfig: String): LoaderSelenium = {
    val loader = new LoaderSelenium(mapper.readValue(getLines(inConfig), classOf[YamlConfig]))
    return loader
  }

  var webDriver: WebDriver = null

  def getWebDriver: WebDriver = {
    if webDriver != null then {
      sequenceId = 0
      return webDriver
    }
    val options = new ChromeOptions();
    options.addArguments("--headless")
    options.addArguments("--disable-gpu")
    options.addArguments("--no-sandbox")
    options.addArguments("--window-size=1400,800")
    options.addArguments("--disable-dev-shm-usage")
    options.addArguments("--shm-size=2g")
    webDriver = ChromeDriver(options);
    return webDriver
  }

  var sequenceId: Int = 0

  def getId: Int = {
    sequenceId += 1
    return sequenceId
  }

  def close(): Unit = {
    if webDriver == null then return
    webDriver.close()
  }
}