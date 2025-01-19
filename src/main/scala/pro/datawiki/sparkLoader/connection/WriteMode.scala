package pro.datawiki.sparkLoader.connection

enum WriteMode {
  case append, overwrite

  override def toString: String = {
    this.ordinal match
      case 0 => return "append"
      case 1 => return "overwrite"
      case _ => throw Exception()
  }
}
