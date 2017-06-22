package parser

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import schema.SchemaBuilder

object ParquetRecordUtil {

  /**
   * Generate an RDD of Parquet Records and the associated StructType from an RDD of JSON strings.
   *
   * @param inputJsons An RDD of JSON Strings, specific to ShopKick's Log Event Format
   * @return StructType of the Schema, and an RDD of all the Rows.
   */
  def GenerateParquetRecords(inputJsons: RDD[String]): (StructType, RDD[Row]) = {
    //Flatten the RDD and explode out the keys.
    val myFlattenedRDD = inputJsons.flatMap(ShopKickSpecificJsonParserUtil.parseAndFlattenJsonEvents(_))

    //Generate a generalized Avro schema by inferring from each event.
    val schemaBuilder = new SchemaBuilder("my.name.space")
    val serializedSchema = schemaBuilder.inferFrom(myFlattenedRDD)

    //Generate GenericData.Records from the key-values and the schema inferred previously.
    val myRecords = myFlattenedRDD
      .map(_.values.asInstanceOf[Map[String, Object]])
      .map(x => {
      //Deserialize the schema
      val schema = Schema.parse(serializedSchema)
      GenerateParquetRow(x, schema)
    })
    (convertSchemaToStructTypeAndOverrideInt(Schema.parse(serializedSchema)), myRecords)
  }

  /**
   * Generates a Parquet row from the logMap, using the specified Avro Schema.
   *
   * @param logMap A Map of column names mapped to their corresponding object.
   * @param schema The Avro Schema used to generate the Row object.
   * @return A Row containing both the data and the schema.
   */
  protected [parser] def GenerateParquetRow (logMap: Map[String, Object], schema: Schema): Row = {
    val recordFields = schema.getFields()
    val recordSize = recordFields.size()
    val array = new collection.mutable.ArraySeq[Object](recordSize)
    val structType = convertSchemaToStructTypeAndOverrideInt(schema)

    //TODO: Note that there is a risk of data loss here. In the short term it works for numbers that we would reasonably
    //TODO: expect to see. A longer term change would be to change the output format to store larger output numbers (ie: INT64)
    //TODO: This is due to the Json4s parser wrapping JInt values as scala.math.BigInt.
    structType.fieldNames.zipWithIndex.foreach(element => {
      val datum = logMap.getOrElse(element._1, null)

      // NOTE: We need to drop the BigInt and BigDecimal types used by the json4s parser,
      // as they are not compatible with Parquet format.
      val recordToWrite =
        datum match {
          case elem : scala.math.BigInt =>
            if (elem.isValidLong)
              elem.toLong.asInstanceOf[AnyRef]
            else
              throw new NumberFormatException(s"BigInt $elem is outside of Int range while casting to Integer. " +
                s"Review Input data or modify code to accept larger numbers")
          case elem : scala.math.BigDecimal =>
            if (elem.isDecimalDouble)
              elem.toDouble.asInstanceOf[AnyRef]
            else
              throw new NumberFormatException(s"BigInt $elem is outside of Double range while casting to Double. " +
                s"Review Input data or modify code to accept larger numbers")
          case _ => datum
        }
      array.update(element._2, recordToWrite)
    })

    val genRow = new GenericRowWithSchema(array.toArray, structType)
    genRow
  }

  /**
   * Convert the Schema to a parquet format, and change all the 4-bit integer to 8-bit long.
   *
   * @param schema The Avro Schema
   * @return
   */
  protected [parser] def convertSchemaToStructTypeAndOverrideInt(schema: Schema): StructType = {
    val parquetSchema = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]

    //Change the inferred Int type to Long. Though technically since bigInt is theoretically much bigger than Long,
    //I would not expect to get such large numbers coming in from a beacon. If I am wrong then this will need to be changed.
    val modifiedParquetFields = parquetSchema.map(foo => {
      foo.dataType match {
        case field : spark.sql.types.IntegerType => new StructField(foo.name, LongType, foo.nullable, foo.metadata)
        case _ => foo
      }
    })
    new StructType(modifiedParquetFields.toArray)
  }
}