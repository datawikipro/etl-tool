package pro.datawiki.exception

class TableNotExistException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)

  def this() = this("Table does not exist", null)
}
