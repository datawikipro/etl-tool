package pro.datawiki.sparkLoader.connection

enum WriteMode {
  case append, overwrite, merge,stream,streamByRunId

  def toSparkString: String = {
    this.ordinal match{
            case 0 => return "append"
            case 1 => return "overwrite"
            case 2 => return "merge"
            case 3 => return "append"
            case 4 => return "append"
            case _ => {
        throw Exception()
            }
      }
  }
  
  def getExportString: String = {
    this.ordinal match
      case 0 => return "append"
      case 1 => return "overwrite"
      case 2 => return "merge"
      case 3 => return "stream"
      case _ => {
        throw Exception()
      }
  }
  
}

object WriteMode {
  def apply(in: String): WriteMode = {
    in match {
      case "overwrite" => return WriteMode.overwrite
      case "append" => return WriteMode.append
      case "stream" => return WriteMode.stream
      case "merge" => return WriteMode.merge
      case "streamByRunId" => return WriteMode.streamByRunId
      case _ => {
        throw Exception()
      }
    }
  }
}