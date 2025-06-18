package pro.datawiki.sparkLoader.connection

enum WriteMode {
  case append, overwrite, merge

  override def toString: String = {
    this.ordinal match
      case 0 => return "append"
      case 1 => return "overwrite"
      case 2 => throw Exception()
      case _ => throw Exception()
  }
}
