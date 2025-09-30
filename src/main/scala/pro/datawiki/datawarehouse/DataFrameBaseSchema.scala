package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.DataProcessingException
import pro.datawiki.schemaValidator.baseSchema.*


case class DataFrameBaseSchema(partition: String,
                               df: BaseSchemaArray,
                               validData: Boolean) extends DataFrameTrait {
  var locDf: BaseSchemaArray = df

  override def isEmpty: Boolean = {
    if df == null then return true
    return false
  }

  override def getFullName(in: String): String = {
    if partition == null then return s"${in}"
    return s"${in}__$partition"
  }

  override def getDataFrame: DataFrame = df.packageDataFrame

  override def isValidData: Boolean = validData

  override def getPartitionName: String = partition

  private def aaa(col: BaseSchemaStruct, key: String, value: String): BaseSchemaObject = {

    col match {
      case x: BaseSchemaObject => {
        BaseSchemaObject(x.inElements ::: List((key, BaseSchemaString(value, false))), false)
      }
      case other => {
        throw DataProcessingException(s"Unsupported data type for constant column addition: ${other.getClass.getName}")
      }
    }
  }

  override def addConstantColumn(name: String, column: String): Unit = {
    val newBaseElement: BaseSchemaTemplate = locDf.baseElement match {
      case x: BaseSchemaObjectTemplate => {
        BaseSchemaObjectTemplate(
          x.inElements ::: List((name, BaseSchemaStringTemplate(false))),
          false)
      }
      case other => {
        throw DataProcessingException(s"Unsupported base element type for constant column: ${other.getClass.getName}")
      }
    }

    var list: List[BaseSchemaStruct] = locDf.list.map(col => aaa(col, name, column))

    locDf= BaseSchemaArray(list, newBaseElement, false)
  }

  override def unionAll(add: DataFrameTrait): DataFrameTrait = {
    add match {
      case x: DataFrameBaseSchema => {
        val bs = BaseSchemaArray(
          locDf.list ::: x.locDf.list,
          locDf.baseElement.fullMerge(x.locDf.baseElement),
          true
        )

        return DataFrameBaseSchema(partition, bs, validData)
      }
      case fs => {
        throw DataProcessingException(s"Unsupported DataFrame type for union: ${fs.getClass.getName}")
      }
    }
  }
}

object DataFrameBaseSchema {
  def apply(partition: String, bs: BaseSchemaArray, validData: Boolean = true): DataFrameBaseSchema = {
    if bs == null then {
      throw DataProcessingException("DataFrame cannot be null")
    }
    return new DataFrameBaseSchema(partition, bs, validData)
  }
}