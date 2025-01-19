package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.{DataFrame, Row}
import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium.getWebDriver
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}

import java.time.Duration


class LoaderSelenium(configYaml: YamlConfig) extends ConnectionTrait {

  def run(row: Row): DataFrame = {
    var newConfigYaml: YamlConfig = configYaml.copy
    if row != null then {
      row.schema.fields.foreach(i => {
        newConfigYaml.modifyConfig(i.name, row.get(row.fieldIndex(i.name)).toString)
      })
    }

    val webDriver = getWebDriver
    //Open web application

    webDriver.get(newConfigYaml.getUrl)
    webDriver.manage.timeouts.implicitlyWait(Duration.ofSeconds(5))

    val html: WebElement = webDriver.findElement(By.tagName("html"))
    var result: List[KeyValue] = List.apply()
    //try {
      newConfigYaml.getTemplate.foreach(i => {
        val a = i.getSubElements(html)
        result = result ::: a
      })
    //} catch
//      case _ => println("skip")
    val list: Seq[Row] = Seq.apply(newConfigYaml.convertToSchema(result))

    val rowsRDD = SparkObject.spark.sparkContext.parallelize(list)//.map(Row.fromSeq)

    val df: DataFrame = SparkObject.spark.createDataFrame(rowsRDD, newConfigYaml.getSchema)
    df.printSchema()
    df.show()
    return df
  }
}

object LoaderSelenium extends YamlClass {
  def apply(inConfig: String): LoaderSelenium = {
    val loader = new LoaderSelenium(mapper.readValue(getLines(inConfig), classOf[YamlConfig]))
    return loader
  }

  var webDriver: WebDriver = null
  var pageStart = 0

  def getWebDriver: WebDriver = {
    pageStart += 1
    println(pageStart)
    if webDriver != null then {
      sequenceId = 0
      Thread.sleep(2000)
      return webDriver
    }
    val options = new ChromeOptions();
    options.addArguments("--headless")
    options.addArguments("--disable-gpu")
    options.addArguments("--no-sandbox")
    options.addArguments("--window-size=1400,800")
    webDriver = ChromeDriver(options);
    //Creating webdriver instance
    webDriver.manage.timeouts.implicitlyWait(Duration.ofSeconds(5))
    return webDriver
  }

  var sequenceId: Int = 0

  def getId: Int = {
    sequenceId += 1
    return sequenceId
  }
}