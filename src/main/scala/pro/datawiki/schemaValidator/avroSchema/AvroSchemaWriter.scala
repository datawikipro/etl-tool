package pro.datawiki.schemaValidator.avroSchema

import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
import pro.datawiki.exception.{NotImplementedException, SchemaValidationException}
import pro.datawiki.schemaValidator.baseSchema.*

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/**
 * Класс для преобразования структур BaseSchemaTemplate в формат Avro Schema
 */
object AvroSchemaWriter {

  /**
   * Преобразует BaseSchemaTemplate в строку Avro Schema.
   *
   * @param schema     Структура схемы для преобразования
   * @param namespace  Пространство имен для схемы Avro
   * @param recordName Имя корневой записи для схемы Avro
   * @param docString  Документация для схемы
   * @return Строка, содержащая схему Avro
   */
  def convertToAvroSchema(schema: BaseSchemaTemplate,
                          namespace: String = "com.datawiki",
                          recordName: String = "GeneratedRecord",
                          docString: String = "Автоматически сгенерированная Avro схема"): String = {
    val avroSchema = generateAvroSchema(schema, namespace, recordName, docString)
    avroSchema.toString(true) // Возвращаем схему в виде отформатированной строки JSON
  }

  /**
   * Генерирует объект Avro Schema для заданной структуры схемы.
   *
   * @param schema     Структура схемы для преобразования
   * @param namespace  Пространство имен для схемы Avro
   * @param recordName Имя записи для схемы Avro
   * @param docString  Документация для схемы
   * @return Объект Schema, представляющий Avro схему
   */
  def generateAvroSchema(schema: BaseSchemaTemplate,
                         namespace: String,
                         recordName: String,
                         docString: String): Schema = {
    schema match {
      case obj: BaseSchemaObjectTemplate => generateObjectSchema(obj, namespace, recordName, docString)
      case arr: BaseSchemaArrayTemplate => generateArraySchema(arr, namespace, recordName + "Element", docString)
      case str: BaseSchemaStringTemplate => Schema.create(Type.STRING)
      case int: BaseSchemaIntTemplate => Schema.create(Type.LONG)
      case dbl: BaseSchemaDoubleTemplate => Schema.create(Type.DOUBLE)
      case bool: BaseSchemaBooleanTemplate => Schema.create(Type.BOOLEAN)
      //      case null_: BaseSchemaNullTemplate => Schema.create(Type.NULL)
      case _ => {
        // Для неопознанных типов возвращаем строковый тип
        //        Schema.create(Type.STRING)
        throw NotImplementedException("Method not implemented")
      }
    }
  }

  /**
   * Генерирует схему Avro для объектного типа.
   *
   * @param schema     Шаблон объекта
   * @param namespace  Пространство имен для схемы Avro
   * @param recordName Имя записи для схемы Avro
   * @param docString  Документация для схемы
   * @return Объект Schema, представляющий объектную схему
   */
  private def generateObjectSchema(schema: BaseSchemaObjectTemplate,
                                   namespace: String,
                                   recordName: String,
                                   docString: String): Schema = {
    // Создаем запись Avro
    val record = Schema.createRecord(recordName, docString, namespace, false)

    // Создаем список полей для записи
    val fields = schema.inElements.map { case (name, elemSchema) =>
      // Определяем, является ли поле опциональным
      val fieldSchema = if (elemSchema.isIgnorable) {
        // Для опциональных полей создаем объединение NULL и фактического типа
        val actualSchema = generateAvroSchema(elemSchema, namespace, recordName + capitalize(name), docString)
        val unionTypes = List(Schema.create(Type.NULL), actualSchema).asJava
        Schema.createUnion(unionTypes)
      } else {
        // Для обязательных полей используем фактический тип
        generateAvroSchema(elemSchema, namespace, recordName + capitalize(name), docString)
      }

      // Создаем поле Avro
      new Field(name, fieldSchema, s"Поле $name", null)
    }.toList.asJava

    // Устанавливаем поля для записи
    record.setFields(fields)
    record
  }

  /**
   * Генерирует схему Avro для массива.
   *
   * @param schema     Шаблон массива
   * @param namespace  Пространство имен для схемы Avro
   * @param recordName Имя записи для элементов массива
   * @param docString  Документация для схемы
   * @return Объект Schema, представляющий схему массива
   */
  private def generateArraySchema(schema: BaseSchemaArrayTemplate,
                                  namespace: String,
                                  recordName: String,
                                  docString: String): Schema = {
    // Генерируем схему для элементов массива
    val itemSchema = generateAvroSchema(schema.getBaseElement, namespace, recordName, docString)

    // Создаем схему массива с указанным типом элементов
    Schema.createArray(itemSchema)
  }

  /**
   * Делает первую букву строки заглавной.
   *
   * @param str Исходная строка
   * @return Строка с заглавной первой буквой
   */
  private def capitalize(str: String): String = {
    if (str == null || str.isEmpty) str
    else str.substring(0, 1).toUpperCase + str.substring(1)
  }
}
