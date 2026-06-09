package pro.datawiki.sparkLoader.context

import pro.datawiki.exception.ConnectionException
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.security.MessageDigest
import scala.collection.mutable
import scala.util.matching.Regex

object ApplicationContext {
  private var connections: mutable.Map[String, ConnectionTrait] = mutable.Map()
  private var globalVariable: mutable.Map[String, String] = mutable.Map()
  private var partitionList: List[String] = List.empty

  def closeConnections(): Unit = {
    connections.foreach(i => i._2.close())
  }

  def setConnection(connectionName: String, connectionTrait: ConnectionTrait): Unit = {
    connections += (connectionName, connectionTrait)
  }

  def getConnection(in: String): ConnectionTrait = {
    try {
      return connections(in)
    } catch
      case _ =>
        throw new ConnectionException(s"Connection $in not initialize")
  }

  // RunId methods
  def setRunId(id: String): Unit = {
    globalVariable += ("runId", id)
    globalVariable += ("run_id", id)
    val patterns: List[Regex] = List.apply(
      """^scheduled__(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\+(\d{2}):(\d{2})$""".r,
         """^manual__(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\+(\d{2}):(\d{2})$""".r

    )
    patterns.foreach(pattern => {
      if pattern.matches(id) then {
        for (patternMatch <- pattern.findAllMatchIn(id)) {
          val result = s"""${patternMatch.group(1)}-${patternMatch.group(2)}-${patternMatch.group(3)} ${patternMatch.group(4)}_${patternMatch.group(5)}_${patternMatch.group(6)}"""
          setGlobalVariable("locationBasedOnRunId", result)
          return
        }
      }
    })

    val md = MessageDigest.getInstance("SHA-256")
    val result = md.digest(id.getBytes("UTF-8")).map("%02x".format(_)).mkString
    setGlobalVariable("locationBasedOnRunId", s"$result")

  }

  def getRunId: String = {
    try {
      return globalVariable("runId")
    } catch
      case _ =>
        throw new Exception(s"Run id is not initialize")
  }

  def setGlobalVariable(key: String, value: String): Unit = {
    globalVariable += (key, value)
  }

  def getGlobalVariable(key: String): String = {
    try {
      return globalVariable(key)
    } catch
      case _ =>
        throw new Exception(s"Run id is not initialize")
  }

  def getPartitions(in: String*): List[String] = {
    in.map(col => getGlobalVariable(
      if col == "runId" || col == "run_id" then "locationBasedOnRunId" else col
    )).toList
  }

  def getPartitions2(in: String*): List[(String, String)] = {
    in.map(col => (col, getGlobalVariable(
      if col == "runId" || col ==  "run_id" then "locationBasedOnRunId" else col
    ))).toList
  }

  def getGlobalVariables:Map[String, String] = globalVariable.toMap

}
