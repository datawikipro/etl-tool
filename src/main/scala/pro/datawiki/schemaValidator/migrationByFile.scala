package pro.datawiki.schemaValidator

import pro.datawiki.yamlConfiguration.YamlClass

/**
 * Основной метод для запуска валидатора JSON схемы.
 * Принимает JSON строку, строит схему и выводит ее в формате YAML.
 *
 * @param jsonFile Входная JSON строка для анализа
 */
@main
def migrationByFile(inJsonFile: String, inSystem: String, outSystem: String): Unit = {

  val inMigration: Migration = Migration.getSystem(inSystem)
  val outMigration: Migration = Migration.getSystem(outSystem)
  val a = inMigration.readSchema(YamlClass.getLines(inJsonFile))
  val b = outMigration.writeTemplate(a.getTemplate)
  YamlClass.writefile(s"$inJsonFile".replace("/input","/output/"), b)

}
