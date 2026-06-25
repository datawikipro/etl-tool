package pro.datawiki.sparkLoader.taskTemplate

case class TaskTemplateIdMapConfig(systemCode: String,
                                   columnNames: List[String],
                                   domainName: String,
                                   timeColumn: String,
                                   secondForExpire: Int,
                                   tableLocation: Option[String] = None
                                  ) {
  def getResolvedLocation: String = {
    tableLocation.getOrElse(s"idmap.db/$domainName")
  }
}
