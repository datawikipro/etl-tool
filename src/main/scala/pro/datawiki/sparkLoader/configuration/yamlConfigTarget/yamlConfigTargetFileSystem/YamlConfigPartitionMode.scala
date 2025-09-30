package pro.datawiki.sparkLoader.configuration.yamlConfigTarget.yamlConfigTargetFileSystem

enum YamlConfigPartitionMode {
  case direct, stream, streamByRunId, none, auto
}

object YamlConfigPartitionMode {
  def apply(in: String): YamlConfigPartitionMode = {
    in match {
      case "direct" => return direct
      case "stream" => return stream
      case "streamByRunId" => return streamByRunId
      case "none" => return none
      case "auto" => return auto

      case _ => throw UnsupportedOperationException("Unsupported partition mode")
    }
  }
}