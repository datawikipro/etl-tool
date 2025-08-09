package pro.datawiki.diMigration.file

import java.nio.file.{Files, Paths}

object FileUtil {
  def fileToString(in: String): String = {
    Files.readString(Paths.get(in))
  }

}
