package pro.datawiki.yamlConfiguration

import com.fasterxml.jackson.annotation.JsonIgnore

class LogicClass {
  @JsonIgnore
  def getLogic(in: Any*): Any = {
    val list = in.toList.filter(_ != null)

    list.length match
      case 1 => return list.head
      case 0 =>
        throw Exception()
      case _ =>
        throw Exception()
  }
}
