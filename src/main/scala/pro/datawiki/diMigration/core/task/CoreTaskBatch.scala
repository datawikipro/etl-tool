package pro.datawiki.diMigration.core.task

import pro.datawiki.exception.NotImplementedException

trait CoreTaskBatch {
  def getTaskId: String = throw NotImplementedException("CoreTaskBatch.getTaskId method not implemented")

}
