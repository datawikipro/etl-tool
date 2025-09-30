package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium

class TaskTemplateSelenium(source: LoaderSelenium) extends TaskTemplate {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    List.apply(source.getDataFrame(parameters, isSync))
  }
}
