package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait

case class YamlConfigTransformationExtractSchema(
                                                  tableName: String,
                                                  jsonColumn: String
                                                ) extends YamlConfigTransformationTrait {
  override def getDataFrame: DataFrame = {
    val list = SparkObject.spark.sql(s"""select $jsonColumn as json_column from $tableName""").collect()
    val schema = StructType(Seq(
      StructField("json", StringType, nullable = false)
    ))
    var a: List[String] = List.apply()
    list.foreach(i => {
      val str = i.get(0).toString.replace("\"", "\\\"")
      //      a = a.appended(str)
      val lst = SparkObject.spark.sql(s"""select schema_of_json("${str}") as json_column""").first().get(0).toString
      println(lst)
      println(1)
      //      val data = List(List(str))
      //      val rdd:RDD[Row] = SparkObject.spark.sparkContext.parallelize(data).map(Row.fromSeq)
      //
      //      val df2 = SparkObject.spark.createDataFrame(rdd, schema)
      //      df2.printSchema()
      //      df2.show()
      //      val df3 = df2.select(
      //        df2.col("json"),
      //        schema_of_json(lit("""{"messageId":8971123,"transactionId":2568149407,"jurisdiction":"ITALY","product":"BACKOFFICE","operationType":"MANUAL_ADJUSTMENT_DOWN","cashSourceInfo":null,"punterInformation":{"punterId":3405160,"country":"IT"},"amount":0.280000,"resultBalance":0.000000,"currency":"EUR","defaultCurrencyAmount":0.280000,"defaultCurrencyResultBalance":0.000000,"defaultCurrency":"EUR","state":"FINISHED","userLogin":"dkishchenko","timestamp":1734076046236,"constructMessageTimestamp":1734076046241,"properties":{"TYPE_MA":"TEST_ACCOUNT","SEANCE_SOURCE":"CALLCENTRE","TRANSACTION_REFERENCE":null,"MA_REASON_FULL_DESCRIPTION":"/ADJUSTMENT DOWN/ACCOUNT ACTIONS/ITALY CLOSURE "}}""".stripMargin)),
      //        schema_of_json(df2.col("json"))
      //      )
      //      df3.printSchema()
      //      df3.show()
    })
    //    val pw = new PrintWriter(new File("config/tmp/a.json"))
    //    try pw.write(a.mkString("\n")) finally pw.close()
    //    val df1 = SparkObject.spark.read.json("config/tmp/a.json")
    //    df1.printSchema()
    //    df1.show()
    //df.show()
    //return df
    throw Exception()
  }

}
