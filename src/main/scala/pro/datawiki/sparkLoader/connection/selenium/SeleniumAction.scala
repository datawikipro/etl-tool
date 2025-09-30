package pro.datawiki.sparkLoader.connection.selenium

enum SeleniumAction {
  case click, none
}

object SeleniumAction {
  def apply(in: String): SeleniumAction = {
    in match {
      case "click" => click
      case null => none
      case _ => throw UnsupportedOperationException("Unsupported Selenium action")
    }
  }
}