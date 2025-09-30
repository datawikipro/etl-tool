package pro.datawiki.sparkLoader.dictionaryEnum

import pro.datawiki.exception.IllegalArgumentException

enum WriteMode {
  case append, overwriteTable, overwritePartition, merge, stream, streamByRunId

  def toSparkString: String = {
    this match {
      case `append` => return "append"
      case `overwriteTable` => return "overwrite"
      case `overwritePartition` => return "overwrite"
      case `merge` => return "merge"
      case `stream` => return "append"
      case `streamByRunId` => return "append"
      case _ => {
        throw IllegalArgumentException("Unsupported WriteMode ordinal")
      }
    }
  }

  def getExportString: String = {
    this match {
      case `append` => return "append"
      case `overwriteTable` => return "overwriteTable"
      case `overwritePartition` => return "overwritePartition"
      case `merge` => return "merge"
      case `stream` => return "stream"
      case `streamByRunId` => return "streamByRunId"
      case _ => {
        throw IllegalArgumentException("Unsupported WriteMode ordinal")
      }
    }
  }

}

object WriteMode {
  def apply(in: String): WriteMode = {
    in match {
      case "overwrite" => return WriteMode.overwriteTable
      case "overwriteTable" => return WriteMode.overwriteTable
      case "overwritePartition" => return WriteMode.overwritePartition
      case "append" => return WriteMode.append
      case "stream" => return WriteMode.stream
      case "merge" => return WriteMode.merge
      case "streamByRunId" => return WriteMode.streamByRunId
      case _ => {
        throw IllegalArgumentException(s"Unsupported WriteMode: $in")
      }
    }
  }
}