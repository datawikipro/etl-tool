package pro.datawiki.sparkLoader.task.taskAdHoc

case class TaskAdHocParameters(list: List[TaskAdHocParameter]) {
  def getMap:Map[String, String] = {
    val a = list.map(col => (col.key, col.value)).toMap
    return a
  }
}