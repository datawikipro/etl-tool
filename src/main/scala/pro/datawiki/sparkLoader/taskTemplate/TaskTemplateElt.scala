package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse

class TaskTemplateElt(sql: List[String],
                      connection: ConnectionTrait) extends TaskTemplate {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    connection match {
      case x: LoaderClickHouse => {
        sql.foreach(i => {
          try {
            if !x.runSQL(i) then {
              throw NotImplementedException("Method not implemented")
            }
          } catch {
            case e: Exception => {
              println(i)
              throw Exception(e)
            }
          }
        })
      }
      case fs => {
        throw NotImplementedException("Method not implemented")
      }
    }
    return List.empty
  }

}
