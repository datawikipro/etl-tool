package pro.datawiki.diMigration

import pro.datawiki.diMigration.core.dictionary.OutputSystem
import pro.datawiki.diMigration.input.base.LoadObject
import pro.datawiki.sparkLoader.configuration.EltConfig.{getLines, mapper}
import pro.datawiki.yamlConfiguration.YamlClass

class AttributeYamlTransform(
                               source: AttributeYamlSource,
                               target: AttributeYamlTarget
                              ) {
  def process(): Unit = {
    val load: LoadObject = source.getLoadObject

    if source.getList.nonEmpty then load.intersect(source.getList)

    load.diff(Files.getListFilesInFolder(target.getLocation, target.getPostfix))

    load.run(source, target)

  }
}