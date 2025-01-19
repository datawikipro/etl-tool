package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.connection.{Connection, DatabaseTrait}

import scala.jdk.CollectionConverters.*

class TransformationIdMap(domainName: String,
                          rkKey:String,
                          systemCode:String,
                          isGenerated: Boolean,
                          columnNames: List[String]) {
  def addendNewKeys(df: DataFrame): DataFrame = {
    var existsKeys = TransformationIdMap.connect.getIdmapDataFrame(domainName, systemCode)
    val ccdKey = concat_ws(TransformationIdMap.constantSeparator, columnNames.map(col): _*).as("ccd")
    val keys = df.
      select(ccdKey).
      where("""ccd is not null and ccd != """"").
      join(existsKeys, "ccd", "left").
      where("rk is null").
      distinct()
    
    TransformationIdMap.connect.insertCCdToIdmap(keys, domainName, systemCode, false)
    if isGenerated then {
      //throw Exception()
    }
    existsKeys = TransformationIdMap.connect.getIdmapDataFrame(domainName, systemCode)

    var df2 = df
    if !(columnNames.length==1 && columnNames.head == "ccd") then {
      df2 = df2.withColumn("ccd", ccdKey)
    }
    df2 = df2.join(existsKeys, "ccd", "left").select(df.col("*"),existsKeys.col("rk").as(rkKey))

    return df2
  }

  def addNewKeys(df: DataFrame):Unit = {
    TransformationIdMap.connect.insertCCdToIdmap(df, domainName, systemCode, true)
  }

  def getCCDByRk:DataFrame = {
    return TransformationIdMap.connect.getIdmapDataFrame(domainName, systemCode)
  }
  
}

object TransformationIdMap {
  val constantSeparator: String = "!@#"
  var connect: DatabaseTrait = null
  
  def apply(domainName: String,
            rkKey:String,
            systemCode:String,
            isGenerated: Boolean,
            columnNames: List[String]): TransformationIdMap = {
    return new TransformationIdMap(domainName=domainName,rkKey=rkKey,systemCode=systemCode, isGenerated=isGenerated, columnNames=columnNames)
  }

  def setIdmap(in: String): Unit = {
    if in  == null then {
      return 
    }

    Connection.getConnection(in)  match
        case x: DatabaseTrait => connect = x
        case _ => throw Exception()
    
  }
}