package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.DataFrame
import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.schemaValidator.sparkRow.{SparkRow, SparkRowAttribute}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium.getWebDriver
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.task.Context
import pro.datawiki.sparkLoader.transformation.TransformationCacheFileStorage
import pro.datawiki.yamlConfiguration.YamlClass

import java.time.Duration
import java.util.Base64
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class LoaderSelenium(configYaml: YamlConfig) extends ConnectionTrait {
  private var cache: TransformationCacheFileStorage = null

  def set(inCache: String): Unit = {
    val con = Context.getConnection(inCache)
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

  private def getDataFrameFromCustom(inSeleniumList: Map[String, SeleniumType], inYamlConfig: YamlConfig): DataFrame = {
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

  private def getDataFrameFromWebDriver(inWebDriver: WebDriver, newConfigYaml: YamlConfig, isSync: Boolean): DataFrameTrait = {
    var df: DataFrame = null
    val html: WebElement = inWebDriver.findElement(By.tagName("html"))

    val result: Map[String, SeleniumType] = newConfigYaml.process(html)

    if configYaml.getSchema.length == 1 then {
      if configYaml.getSchema.head.getType == "json" then {
        throw Exception() //TODO
        //        result.getList.head.value.getValue match
        //          case x: String => df = getDataFrameFromJson(x)
        //          case _ => throw Exception()

      } else {
        df = getDataFrameFromCustom(result, newConfigYaml)
      }
    } else {
      df = getDataFrameFromCustom(result, newConfigYaml)
    }

    LogMode.debugDF(df)
    return DataFrameOriginal(df)
  }

  def getDataFrameFromData(html: String): DataFrameTrait = {
    val driver = getWebDriver(false)

    val encoded = Base64.getEncoder.encodeToString(html.getBytes("UTF-8"))
    driver.get(s"data:text/html;base64,$encoded")

    val result = getDataFrameFromWebDriver(driver, configYaml, false)
    driver.quit()
    return result
  }

  def getDataFrame(row: mutable.Map[String, String], isSync: Boolean): DataFrameTrait = {
    val webDriver: WebDriver = getWebDriver(isSync)
    val newConfigYaml: YamlConfig = YamlConfig.apply(in = configYaml, row = row)
    webDriver.get(newConfigYaml.getUrl)

    val wait = new WebDriverWait(webDriver, Duration.ofSeconds(5))
    //val rawField = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("root")));

    val rawField = wait.until(ExpectedConditions.presenceOfElementLocated(By.tagName("html")));
    wait.until((d) => rawField.getDomProperty("innerHTML").nonEmpty)

    val result = getDataFrameFromWebDriver(webDriver, newConfigYaml, isSync)
    if !isSync then webDriver.quit()
    return result
  }

  override def close(): Unit = {
    LoaderSelenium.close()
    if cache != null then cache.close()
  }
}

object LoaderSelenium extends YamlClass {
  def apply(inConfig: String): LoaderSelenium = {
    try {
      val loader = new LoaderSelenium(mapper.readValue(getLines(inConfig), classOf[YamlConfig]))
      return loader
    } /*catch
      case e: Error => throw Exception(e)*/
  }

  var webDriver: WebDriver = null

  private val lock = new ReentrantLock()

  private def getNewWebDriver: WebDriver = {
    lock.lock()
    try {
      val options = new ChromeOptions()
//      options.addArguments("--headless")
      options.addArguments("--disable-gpu")
      options.addArguments("--no-sandbox")
      options.addArguments("--window-size=1400,800")
      options.addArguments("--disable-dev-shm-usage")
      options.addArguments("--shm-size=2g")
//      options.addArguments("--incognito")
      var newWebDriver = ChromeDriver(options)
      return newWebDriver
    } finally {
      lock.unlock()
    }
  }

  def getWebDriver(isSync: Boolean): WebDriver = {
    if !isSync then return getNewWebDriver

    if webDriver != null then {
      sequenceId = 0
      return webDriver
    }
    webDriver = getNewWebDriver
    return webDriver
  }

  var sequenceId: Int = 0

  def getId: Int = {
    sequenceId += 1
    return sequenceId
  }

  def close(): Unit = {
    if webDriver == null then return
    webDriver.quit()
    webDriver = null
  }
}