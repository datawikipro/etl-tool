package pro.datawiki.sparkLoader.configuration

object RunConfig {
  private var partition: String = ""

  def setPartition(in: String): Unit = {
    partition = in
  }

  def getPartition: String = {
    throw Exception()
  }

}
