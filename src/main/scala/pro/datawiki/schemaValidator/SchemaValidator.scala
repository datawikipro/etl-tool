package pro.datawiki.schemaValidator

import org.json4s.*
import org.json4s.jackson.JsonMethods.*
import pro.datawiki.sparkLoader.YamlClass

object SchemaValidator extends YamlClass{
  def checkJson(in:String, validatorConfig:String):Boolean = {
    try {
      val json = parse(in)
        val loader = mapper.readValue(getLines(validatorConfig), classOf[Object])
      loader.validateJson(json)
      return true
    }
    catch
      case _=> return false
  }
}
