package pro.datawiki.sparkLoader.connection.qdrant

case class QdrantPoint(
                        id: String,
                        vector: List[Double],
                        payload: Map[String, Any]
                      ) {
  def toJson: String = {
    val vectorJson = vector.mkString("[", ",", "]")
    val payloadJson = payload.map { case (k, v) => s""""$k": ${
      v match {
        case s: String => s""""$s""""
        case n: Number => n.toString
        case b: Boolean => b.toString
        case _ => s""""${v.toString}""""
      }
    }"""
    }.mkString("{", ",", "}")

    s"""{"id": "$id", "vector": $vectorJson, "payload": $payloadJson}"""
  }
}