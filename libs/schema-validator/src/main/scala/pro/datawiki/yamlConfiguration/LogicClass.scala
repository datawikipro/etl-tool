package pro.datawiki.yamlConfiguration

import pro.datawiki.exception.ValidationException

object LogicClass {
  
  def getLogic(in: Any*): Any = {
    var list = in.toList.filter(_ != null)

    if (list.length > 1 && list.exists(!_.isInstanceOf[String])) {
      list = list.filter(!_.isInstanceOf[String])
    }

    list.length match
      case 1 => return list.head
      case 0 =>
        throw new ValidationException("LogicClass.getLogic requires exactly one non-null argument")
      case _ =>
        throw new ValidationException(s"LogicClass.getLogic requires exactly one argument, got: ${list.length}")
  }
}

