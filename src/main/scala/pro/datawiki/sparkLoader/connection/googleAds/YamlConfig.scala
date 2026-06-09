package pro.datawiki.sparkLoader.connection.googleAds

import scala.reflect.io.File

case class YamlConfig(
                       config: String,
                       configAds: String,
                       clientId: String,
                       clientSecret: String,
                       accessToken: String,
                       refreshToken: String,
                       customerId: String
                     )