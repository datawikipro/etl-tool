package pro.datawiki.diMigration

import pro.datawiki.diMigration.core.dictionary.InputSystem
import pro.datawiki.diMigration.input.base.LoadObject
import pro.datawiki.diMigration.input.{LoadSas, LoadYaml}

class AttributeYamlSource(
                           config: String,
                           location: String,
                           list: List[String] = List.apply(),
                         ) {

  def getConfig: InputSystem = {
    config match {
      case "sas" => (InputSystem.Sas)
      case "yaml" => (InputSystem.Yaml)
      case _ => throw Exception()
    }
  }

  def getLoadObject: LoadObject = {
    getConfig match {
      case InputSystem.Sas =>
        LoadSas.getLoadObjects(s"${getLocation}")
      case InputSystem.Yaml =>
        LoadYaml.getLoadObjects(s"${getLocation}")
      case _ => throw Exception()
    }
  }

  def getLocation: String = location

  def getList: List[String] = list
}
