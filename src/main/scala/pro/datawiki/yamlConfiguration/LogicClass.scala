package pro.datawiki.yamlConfiguration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.ValidationException

@JsonInclude(JsonInclude.Include.NON_ABSENT)
class LogicClass {
  
  @JsonIgnore
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
