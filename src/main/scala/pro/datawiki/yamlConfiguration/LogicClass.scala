package pro.datawiki.yamlConfiguration

import pro.datawiki.exception.ValidationException

object LogicClass {
  
  def getLogic(in: Any*): Any = {
    val list = in.toList.filter(_ != null)

    list.length match
      case 1 => return list.head
      case 0 =>
        throw new ValidationException("LogicClass.getLogic requires exactly one non-null argument")
      case _ =>
        throw new ValidationException(s"LogicClass.getLogic requires exactly one argument, got: ${list.length}")
  }
}
