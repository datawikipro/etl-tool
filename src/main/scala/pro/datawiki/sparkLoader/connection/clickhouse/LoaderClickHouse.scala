package pro.datawiki.sparkLoader.connection.clickhouse

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}

import java.util.Properties

class LoaderClickHouse(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait {

  override def getDataFrameBySQL(sql: String): DataFrame = {
    SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
  }

  override def insertCCdToIdmap(df: DataFrame,
                                domainName: String,
                                tenantName: String,
                                hasRk: Boolean): Unit = {
    val tempNameDf=s"""${domainName}_${tenantName}_df"""
    val tempNameMinus=s"""${domainName}_${tenantName}_dfMinus"""
    
    df.createTempView(tempNameDf)
    getIdmapDataFrame(domainName = domainName, tenantName = tenantName).createTempView(tempNameMinus)
    val colList =  hasRk match
      case true =>  List.apply(s"'$tenantName' as tenant", "df.ccd", "df.rk")
      case false => List.apply(s"'$tenantName' as tenant", "df.ccd")
    val sql =
      s"""select ${colList.mkString(", ")}
        | from $tempNameDf df
        | left join $tempNameMinus df_minus using (ccd)
        |where df_minus.rk is null
        |""".stripMargin
    
    val df2 = SparkObject.spark.sql(sql)
    df2.show()
    df2.write.mode("append").jdbc(getJdbc, domainName, getProperties)
  }

  override def getIdmapDataFrame(domainName: String, tenantName: String): DataFrame = {
    return getDataFrameBySQL(s"""select ccd, tenant, rk from $domainName where tenant = '$tenantName'""")
  }

  def getProperties: Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.login)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    return prop
  }

  private def getJdbcDb(db: YamlServerHost): String = {
    return s"jdbc:clickhouse://${db.host}:${db.port}/${db.database}"
  }

  def getJdbc: String = {
    if configYaml.server.replica != null then {
      configYaml.server.replica.foreach(i => {
        return getJdbcDb(i)
      })
    }
    return getJdbcDb(configYaml.server.master)
  }
}

object LoaderClickHouse extends YamlClass {
  def apply(inConfig: String): LoaderClickHouse = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderClickHouse(configYaml)
  }
}