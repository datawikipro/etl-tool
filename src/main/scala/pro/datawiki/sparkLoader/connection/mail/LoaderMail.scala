package pro.datawiki.sparkLoader.connection.mail

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.{ConfigurationException, TableNotExistException}
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.sparkLoader.transformation.TransformationCacheFileStorage
import pro.datawiki.yamlConfiguration.YamlClass

import java.time.{LocalDateTime, ZoneId}
import java.util.{Date, Properties}
import javax.mail.*
import javax.mail.internet.InternetAddress
import javax.mail.search.*

class LoaderMail(configYaml: YamlConfigMail, configLocation: String) extends ConnectionTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating Mail connection")
  private var cache: TransformationCacheFileStorage = null

  var localSelenium: LoaderSelenium = null

  def getSelenium: LoaderSelenium = {
    if localSelenium != null then return localSelenium
    localSelenium = new LoaderSelenium(configYaml.getSeleniumConfig, configLocation)
    return localSelenium
  }

  def set(inCache: String): Unit = {
    val con = ApplicationContext.getConnection(inCache)
    cache = con match
      case x: FileStorageTrait => TransformationCacheFileStorage()
      case other => throw ConfigurationException(s"Неизвестный тип чтения почты: '$other'")
  }

  private def localCache: TransformationCacheFileStorage = {
    if cache == null then throw ConfigurationException("Кэш не был инициализирован в LoaderMail. Вызовите метод set() перед использованием кэша.")
    return cache
  }

  var privateSession: Session = null

  def getSessionMailServer: Session = {
    if !(privateSession == null) then return privateSession
    // Конфигурация свойств подключения
    val useSSL: Boolean = configYaml.protocol match {
      case "imaps" => true
      case "imap" => false
      case other => throw ConfigurationException(s"Неподдерживаемый протокол почты: '$other'. Поддерживаются только 'imap' и 'imaps'.")
    }

    val props = new Properties()
    props.put("mail.store.protocol", configYaml.protocol)
    props.put(s"mail.${configYaml.protocol}.host", configYaml.host)
    props.put(s"mail.${configYaml.protocol}.port", configYaml.port.toString)
    props.put(s"mail.${configYaml.protocol}.ssl.enable", useSSL.toString)

    // Создание сессии
    privateSession = Session.getInstance(props, null)
    return privateSession
  }

  private def getStoreForUser(email: String, password: String): Store = {
    val session = getSessionMailServer
    val store: Store = session.getStore(configYaml.protocol)
    store.connect(email, password)
    return store
  }

  def getDataFrame(email: String,
                   password: String,
                   senderEmail: String,
                   targetSubject: String,
                   time: LocalDateTime): DataFrameTrait = {

    val store = getStoreForUser(email, password)
    val inbox = store.getFolder("INBOX")
    inbox.open(Folder.READ_ONLY)

    var array: Array[SearchTerm] = Array.apply()

    if senderEmail != null then {
      array = array.appended(new FromTerm(new InternetAddress(senderEmail)))
    }
    if targetSubject != null then {
      array = array.appended(new SubjectTerm(targetSubject))
    }
    // Фильтрация по дате (если time не null)
    if time != null then {
      val date = Date.from(time.atZone(ZoneId.systemDefault()).toInstant)
      array = array.appended(new SentDateTerm(ComparisonTerm.GE, date))
    }

    // Поиск писем с фильтрацией на сервере
    val searchTerm = new AndTerm(array)
    val messages = inbox.search(searchTerm)

    if messages.isEmpty then {
      return throw TableNotExistException()
    }

    // Фильтрация и сортировка по дате (на всякий случай, если сервер не поддерживает)
    val filteredMessages = messages.toList
      .filter(m => time == null || m.getSentDate.toInstant.isAfter(time.atZone(ZoneId.systemDefault()).toInstant))
      .sortBy(_.getSentDate)(Ordering[java.util.Date].reverse)

    if filteredMessages.isEmpty then {
      return throw TableNotExistException()
    }

    // Для каждого письма получаем DataFrame и объединяем

    val dfs: List[DataFrame] = filteredMessages.map { msg =>
      val body = msg.getContent.toString
      getSelenium.getDataFrameFromData(body).getDataFrame
    }

    // Объединяем все DataFrame через union
    val resultDf = dfs.reduce(_.unionByName(_))
    return new pro.datawiki.datawarehouse.DataFrameOriginal(resultDf)
  }

  override def close(): Unit = {
    if cache != null then cache.close()
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.mail
  }

  override def getConfigLocation(): String = {
    _configLocation
  }
}

object LoaderMail extends YamlClass {
  def apply(inConfig: String): LoaderMail = {
    try {
      val loader = new LoaderMail(mapper.readValue(getLines(inConfig), classOf[YamlConfigMail]), inConfig)
      return loader
    } catch
      case e: Error => throw ConfigurationException(s"Ошибка при инициализации LoaderMail: ${e.getMessage}", e)
  }

}