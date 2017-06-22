package parser

import org.apache.avro.Schema
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class ParquetRecordUtilTest extends FunSuite {
  val simpleSchema =
    """
      |{
      |  "type" : "record",
      |  "name" : "foobar",
      |  "namespace" : "some.namespace",
      |  "fields" : [ {
      |    "name" : "name",
      |    "type" : [ "null", "string" ],
      |    "default" : null
      |  }, {
      |    "name" : "age",
      |    "type" : "int"
      |  }]
      |}
    """.stripMargin

  test("ParquetRecordUtil.convertSchemaToStructTypeAndOverrideInt successfully converts to StructType") {
    val mySchema = Schema.parse(simpleSchema)
    val myStruct = ParquetRecordUtil.convertSchemaToStructTypeAndOverrideInt(mySchema)

    val structFields = Array(new StructField("name",StringType,true), new StructField("age",LongType,false))
    val expectedStructType = new StructType(structFields)
    assert(expectedStructType.equals(myStruct))
  }

  test("ParquetRecordUtil.GenerateParquetRow successfully creates Row") {
    val NAME = "adam"
    val AGE = 123
    val mySchema = Schema.parse(simpleSchema)
    val myMap :Map[String, Object] = Map("name" -> NAME, "age" -> AGE.asInstanceOf[AnyRef]) //Bit of casting for interface.
    val myRow = ParquetRecordUtil.GenerateParquetRow(myMap, mySchema)

    assert(myRow.length == 2)
    assert(myRow.get(0) == NAME)
    assert(myRow.get(1) == AGE)
  }
}
