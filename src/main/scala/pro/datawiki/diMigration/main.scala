package pro.datawiki.diMigration

@main
def main(configLocation: String): Unit = {
  val sourceConfig: AttributeYaml = AttributeYaml(configLocation)
  
  sourceConfig.transformations.foreach(i=> {
    i.process()
  })
  
}
