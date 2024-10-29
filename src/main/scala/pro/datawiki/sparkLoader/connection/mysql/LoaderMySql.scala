package pro.datawiki.sparkLoader.connection.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.jcraft.jsch.{JSch, Session}
import org.apache.spark.sql.functions.{coalesce, col, hash, md5}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}
import pro.datawiki.sparkLoader.configuration.YamlConfigTarget
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.transformation.TransformationIdMap

import java.nio.file.{Files, Paths}
import java.util.Properties

class LoaderMySql(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait {

  override def getDataFrameBySQL(sql: String): DataFrame = {
    val df = SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
    return df
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
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    return prop
  }

  var session: Session = null

  private def getJdbcDb(db: YamlServerHost, gateway: YamlGateway): String = {
    if gateway == null then {
      return s"jdbc:mysql://${db.host}:${db.port}/${db.database}"
    }
    var jsch: JSch = new JSch();
    session = jsch.getSession(gateway.ssh.user, gateway.ssh.host, gateway.ssh.port.toInt)
    jsch.addIdentity(gateway.ssh.keyLocation)
    JSch.setConfig("PubkeyAcceptedAlgorithms", JSch.getConfig("PubkeyAcceptedAlgorithms") + ",ssh-rsa")
    session.setConfig("StrictHostKeyChecking", "no")
    session.connect()
    val por = session.setPortForwardingL(0, db.host, db.port.toInt)
    val url = "jdbc:mysql://localhost:" + por + s"/${db.database}"
    return url

  }


  def getJdbc: String = {
    configYaml.server.replica.foreach(f = i => {
      return getJdbcDb(i, configYaml.gateway)
    })
    return getJdbcDb(configYaml.server.master, configYaml.gateway)
  }

  def writeDF(df: DataFrame,
              columns: List[YamlConfigTargetColumn],
              ccdColumnName: String,
              targetFile: String,
              columnsLogicKey:List[String]): Unit = {
    var cols1: List[String] = List.apply()
    columns.foreach(i => {
      if !i.isNewCCD then
        cols1 = cols1.appended(i.columnName)
    })
    
    df.select(cols1.map(col): _*).createTempView("newRow")
    
    getDataFrameBySQL(s"select ${columnsLogicKey.mkString(", ")} from $targetFile").createTempView("indb")
    
    var new_df = SparkObject.spark.sql(
      s"""select newRow.* 
         |  from newRow
         |  left join indb using(${columnsLogicKey.mkString(", ")})
         | where indb.${columnsLogicKey.head} is null
         |""".stripMargin)
    

    new_df.write.mode("append").jdbc(getJdbc, targetFile, getProperties)
  }

  def mergeRk(df: DataFrame, tenantName: String, domainName: String, ccdColumnName: String, columnsLogicKey: List[String], targetFile: String): Unit = {
    val idmapNew = TransformationIdMap(
      systemCode = tenantName,
      domainName = domainName,
      rkKey = s"${domainName}_rk",
      isGenerated = true,
      columnNames = List.apply("ccd"))

    val targetDf = getDataFrameBySQL(
      s"""select ${ccdColumnName}, ${columnsLogicKey.mkString(",")}
         |  from ${targetFile}""".stripMargin).
      select(col(s"${ccdColumnName}").as("ccd"), md5(coalesce(columnsLogicKey.map(col): _*)).as("hash"))
    targetDf.show()
    val dfPlusHash = df.select(col(s"${domainName}_rk").as("rk"), md5(coalesce(columnsLogicKey.map(col): _*)).as("hash"))
    dfPlusHash.show()
    val res = dfPlusHash.join(targetDf, "hash").filter("rk is not null")
    res.show()
    idmapNew.addNewKeys(res)
  }

  override def writeDf(location:
                       YamlConfigTarget, 
                       df: DataFrame,
                       autoInsertIdmapCCD: Boolean, 
                       columnsLogicKey: List[String]): Unit = {
    var rkKey: String = null
    var ccd: String = null
    var tenantName: String = null
    var domainName: String = null
    location.columns.foreach(i => {
      if i.isNewCCD then
        ccd = i.columnName
        rkKey = s"${i.domainName}_rk"
        tenantName = i.tenantName
        domainName = i.domainName
    })

    df.show()

    writeDF(df = df, columns = location.columns, ccdColumnName = ccd, targetFile = location.targetFile,columnsLogicKey=columnsLogicKey)
    if autoInsertIdmapCCD then {
      mergeRk(
        df = df, 
        tenantName = tenantName, 
        domainName = domainName, 
        ccdColumnName = ccd, 
        columnsLogicKey = columnsLogicKey, 
        targetFile = location.targetFile)
    }
  }

  override def close(): Unit = {
    if session != null then {
      session.disconnect()
    }
  }
}

object LoaderMySql extends YamlClass  {
  def apply(inConfig: String): LoaderMySql = {

    val lines: String = Files.readString(Paths.get(inConfig))
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    return new LoaderMySql(configYaml)
  }
}