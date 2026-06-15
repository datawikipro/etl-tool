package pro.datawiki.textVariable

import pro.datawiki.exception.DataProcessingException

import java.net.URLEncoder
import scala.util.matching.Regex

object WorkWithText {

  private val placeholderRegex: Regex = """\$\{([^}]+)\}""".r

  def replaceWithoutDecode(in: String, row: Map[String, String], mode: String = "raw"): String = {
    if (in == null) return null
    
    try {
      placeholderRegex.replaceAllIn(in, m => {
        val fullContent = m.group(1)
        val parts = fullContent.split(":", 2)
        val key = parts(0).trim
        val defaultValue = if (parts.length > 1) parts(1) else ""
        
        val resolvedValue = row.get(key)
          .orElse(Option(System.getenv(key)))
          .orElse(Option(System.getProperty(key)))
          .getOrElse {
            if (parts.length > 1) defaultValue else m.matched
          }
          
        val processed = mode match {
          case "raw" => resolvedValue
          case "urlEncode" => URLEncoder.encode(resolvedValue, "UTF-8")
          case _ => throw new Exception()
        }
        
        Regex.quoteReplacement(processed)
      })
    } catch {
      case e: Exception =>
        throw DataProcessingException(s"Error processing text replacement: ${e.getMessage}", e)
    }
  }
}

