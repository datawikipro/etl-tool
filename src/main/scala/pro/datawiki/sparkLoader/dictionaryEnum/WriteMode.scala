package pro.datawiki.sparkLoader.dictionaryEnum

import pro.datawiki.exception.IllegalArgumentException

enum WriteMode {
  case append, autoAppend, overwriteTable, overwritePartition, mergeDelta,mergeFull, stream, streamByRunId,autoOverwrite

  def toSparkString: String = {
    this match {
      case `append` => return "append"
      case `autoAppend` => return "append"
      case `overwriteTable` => return "overwrite"
      case `overwritePartition` => return "overwrite"
      case `mergeDelta` => return "mergeDelta"
      case `mergeFull` => return "mergeFull"
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
      case `autoAppend` => return "autoAppend"
      case `overwriteTable` => return "overwriteTable"
      case `overwritePartition` => return "overwritePartition"
      case `mergeDelta` => return "mergeDelta"
      case `mergeFull` => return "mergeFull"
      case `stream` => return "stream"
      case `streamByRunId` => return "streamByRunId"
      case `autoOverwrite` => return "autoOverwrite"
      case _ => {
        throw IllegalArgumentException("Unsupported WriteMode ordinal")
      }
    }
  }

}

object WriteMode {
  def apply(in: String): WriteMode = {
    in match {
      case "overwrite" => return overwriteTable
      case "overwriteTable" => return overwriteTable
      case "overwritePartition" => return overwritePartition
      case "append" => return append
      case "stream" => return stream
      case "merge" => return mergeDelta//TODO
      case "mergeDelta" => return mergeDelta
      case "mergeFull" => return mergeFull
      case "streamByRunId" => return streamByRunId
      case "autoOverwrite" => return autoOverwrite
      case "autoAppend" => return autoAppend
      case _ => {
        throw IllegalArgumentException(s"Unsupported WriteMode: $in")
      }
    }
  }
}