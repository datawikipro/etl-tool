package pro.datawiki.sparkLoader.connection.jsonApi

case class YamlConfig(
                       url: String = throw IllegalArgumentException("URL is required"),
                       method: String = "Get",
                       cookies: List[KeyValue] = List.apply(),
                       headers: List[KeyValue] = List.apply(),
                       authType: AuthType,
                       body: String = "",
                       resultType: String = "json",
                       formData: List[KeyValue] = List.empty,
                       limit: Int,
                       startOffset: Int,
                       pageCount: String,
                       threadCount: Int = 5,
                       timeoutSeconds: Int = 10
                     ) {

}