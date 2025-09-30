package pro.datawiki.schemaValidator

import pro.datawiki.yamlConfiguration.YamlClass

/**
 * Основной метод для запуска валидатора JSON схемы.
 * Принимает JSON строку, строит схему и выводит ее в формате YAML.
 *
 * @param jsonFile Входная JSON строка для анализа
 */
@main
def migrationByFile(inFile: String, inSystem: String, outSystem: String): Unit = {

  val inMigration: Migration = Migration(inSystem)
  val outMigration: Migration = Migration(outSystem)
  val a = inMigration.readSchema(YamlClass.getLines(inFile))
  val b = outMigration.writeTemplate(a.getTemplate)
  YamlClass.writefile(s"$inFile".replace("/input", "/output/"), b)

}
