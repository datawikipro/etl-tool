package pro.datawiki.sparkLoader.connection.googleAds

import com.google.ads.googleads.lib.GoogleAdsClient
import com.google.ads.googleads.v17.services.SearchGoogleAdsRequest
import com.google.auth.oauth2.{AccessToken, GoogleCredentials, ServiceAccountCredentials, UserCredentials}
import org.apache.hadoop.classification.InterfaceAudience.Private
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.yamlConfiguration.YamlClass
import sttp.client4.*

import java.io.{File, FileInputStream}
import scala.util.{Failure, Success, Try}


class LoaderGoogleAds(in: YamlConfig, configLocation: String) extends ConnectionTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating Google Ads connection")

  val credentialsPath = in.config
  //  val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath))
  val credentials: GoogleCredentials = UserCredentials.newBuilder()
    .setClientId(in.clientId)
    .setClientSecret(in.clientSecret)
    .setAccessToken(AccessToken(in.accessToken, null))
    //    .setRefreshToken(in.refreshToken)
    .build();

  // Initialize Google Ads Client with service account credentials
  val googleAdsClient: GoogleAdsClient = GoogleAdsClient.newBuilder()
    .fromPropertiesFile(new File(in.configAds))
    .setCredentials(credentials)
    .build()

  // Define the query
  val query =
    """
      |SELECT campaign.id, campaign.name, ad_group.id, ad_group.name
      |FROM ad_group
      |WHERE segments.date DURING LAST_30_DAYS
  """.stripMargin

  // Create a request
  val customerId = in.customerId // Replace with your customer ID
  val request: SearchGoogleAdsRequest = SearchGoogleAdsRequest.newBuilder()
    .setCustomerId(customerId)
    .setQuery(query)
    .build()

  // Execute the request
  Try(googleAdsClient.getLatestVersion.createGoogleAdsServiceClient) match {
    case Success(googleAdsServiceClient) =>
      val response = googleAdsServiceClient.search(request)
      response.iterateAll().forEach(row => {
        println(s"Campaign ID: ${row.getCampaign.getId}, " +
          s"Campaign Name: ${row.getCampaign.getName}, " +
          s"Ad Group ID: ${row.getAdGroup.getId}, " +
          s"Ad Group Name: ${row.getAdGroup.getName}")
      })
    case Failure(exception) =>
      println(s"Error occurred: ${exception.getMessage}")
  }

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.googleAds
  }

  override def getConfigLocation(): String = {
    _configLocation
  }
}


object LoaderGoogleAds extends YamlClass {
  def apply(inConfig: String): LoaderGoogleAds = {
    val loader = new LoaderGoogleAds(mapper.readValue(getLines(inConfig), classOf[YamlConfig]), inConfig)

    return loader
  }
}
