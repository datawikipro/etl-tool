package pro.datawiki.sparkLoader.connection

trait FileSystemTrait {
  def getSegments(location: String):List[String]
}
