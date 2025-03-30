package pro.datawiki.sparkLoader.configuration.parent

class LogicClass {
  var counter: Int = 0
  var logic: Any = null

  def reset(): Unit = {
    counter = 0
    logic = null
  }

  def setLogic(in:Any): Unit = {
    if in == null then return 
    logic = in
    counter += 1
  }

  def getLogic: Any = {
    counter match
      case 1 => return logic
      case 0 =>
        throw Exception()
      case _ =>
        throw Exception()

  }

}
