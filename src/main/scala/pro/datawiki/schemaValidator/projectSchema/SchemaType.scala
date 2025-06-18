package pro.datawiki.schemaValidator.projectSchema

enum SchemaType {
  case Object, Array, String, Int, Boolean, Null,Long, Double

  override def toString: String = {
    this.ordinal match
//      case 0 => return "Object"
//      case 1 => return "Array"
      case 2 => return "String"
      case 3 => return "Long"
      case 4 => return "Boolean"
      case 5 => return "String"
      case 6 => return "Long"
      case 7 => return "Double"
      case _ => throw Exception()
  }
}
