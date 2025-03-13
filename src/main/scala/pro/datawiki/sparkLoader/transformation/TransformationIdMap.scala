package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.connection.{Connection, DatabaseTrait}

import scala.jdk.CollectionConverters.*

object TransformationIdMap {
  val constantSeparator: String = "!@#"
  var connect: DatabaseTrait = null

  def setIdmap(in: String): Unit = {
    if in  == null then {
      return 
    }

    Connection.getConnection(in)  match
        case x: DatabaseTrait => connect = x
        case _ => throw Exception()
    
  }
}