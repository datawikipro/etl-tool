package pro.datawiki.sparkLoader.target

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DataWarehouseTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCache

object Target {
  var target: ConnectionTrait = null
  
  def setTarget(in: String): Unit = {
    target = Connection.getConnection(in)
  }

  def writeTarget(targetFile:String, columnsLogicKey:List[String],columns:List[String],mode: WriteMode): Unit = {
    val df = SparkObject.spark.sql("select * from target")
    target match
      case x: DataWarehouseTrait => {
        x.writeDf(targetFile, df, columnsLogicKey,columns, mode)
      }
      case _ => throw Exception()
  }

  def writeTarget(targetFile: String,mode: WriteMode): Unit = {
    val df = SparkObject.spark.sql("select * from target")
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    target match
      case x: DataWarehouseTrait => x.writeDf(targetFile, df, mode)
      case _ => throw Exception()
  }
  
}