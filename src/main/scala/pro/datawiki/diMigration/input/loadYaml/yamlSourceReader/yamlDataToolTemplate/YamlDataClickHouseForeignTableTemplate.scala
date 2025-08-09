package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.dag.{CoreDag, CoreSqlDag}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.YamlDataToolTemplate
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadata
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class YamlDataClickHouseForeignTableTemplate(
                                              taskName: String,
                                              connection: String,
                                              configLocation: String,
                                              tableSchema: String,
                                              tableName: String,

                                            ) extends YamlDataToolTemplate {
  override def getCoreDag: List[CoreDag]= {
    val connectionTrait = DatabaseTrait.apply(connection,configLocation)

    val metadata:TableMetadata = connectionTrait.getTableMetadata(tableSchema,tableName)
    var list:List[String]=List.apply()
    metadata.columns.foreach(i=> {
      list=list.appended(s"""${i.column_name} ${LoaderClickHouse.encodeDataType(i.data_type)}""")
    })
    val sql =s"""CREATE TABLE IF NOT EXISTS $tableSchema._$tableName
                |(
                |    ${list.mkString(",\n    ")}
                |)
                |    ENGINE = PostgreSQL('88.211.233.68:30432', 'dwh_api', '$tableName', 'admin', 'H76q3kng6tsfdpoi90$$&97oj013', '$tableSchema');""".stripMargin
    return List.apply(CoreSqlDag(sql))
  }
}


object YamlDataClickHouseForeignTableTemplate extends YamlClass {
  def apply(inConfig: String, row: mutable.Map[String, String]): YamlDataClickHouseForeignTableTemplate = {
    val configYaml: YamlDataClickHouseForeignTableTemplate = mapper.readValue(getLines(inConfig, row), classOf[YamlDataClickHouseForeignTableTemplate])
    return configYaml
  }
}