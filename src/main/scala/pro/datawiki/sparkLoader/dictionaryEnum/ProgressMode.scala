package pro.datawiki.sparkLoader.dictionaryEnum

import pro.datawiki.exception.NotImplementedException

enum ProgressMode {
  case batch, microBatch, row, stream

  def toString2: String = {
    this match {
      case batch => "batch"
      case _ => throw NotImplementedException(s"toString2 not implemented for: ${this}")
    }
  }
}

object ProgressMode {
  def apply(in: String): ProgressMode = {
    in match {
      case "batch" => batch
      case "microBatch" => microBatch
      case "row" => row
      case "stream" => stream
      case _ => {
        throw IllegalArgumentException(s"Unsupported ProgressMode: '$in'")
      }
    }
  }
}