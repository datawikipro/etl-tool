package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.{DataFrame, Row}
import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium.getWebDriver
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}

import java.time.Duration


class LoaderSelenium(configYaml: YamlConfig) extends ConnectionTrait {

  def run(row: Row):  (DataFrame, String) = {
    var df: DataFrame = null
    val webDriver = getWebDriver
    //Open web application
    val newConfigYaml = YamlConfig.apply(in = configYaml, row = row)
    webDriver.get(newConfigYaml.getUrl)
    webDriver.manage.timeouts.implicitlyWait(Duration.ofSeconds(5))

    val html: WebElement = webDriver.findElement(By.tagName("html"))
    val result: SeleniumList = SeleniumList.apply()

    newConfigYaml.getTemplate.foreach(i => result.appendElements(i.getSubElements(html)))


    val sparkRow = SparkRow.apply(result, newConfigYaml)
    val rowsRDD = SparkObject.spark.sparkContext.parallelize(Seq.apply(sparkRow.getRow))
    df = SparkObject.spark.createDataFrame(rowsRDD, sparkRow.getSchema)
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return (df, null)
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