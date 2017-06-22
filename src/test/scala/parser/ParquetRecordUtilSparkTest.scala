package parser

import common.SparkTestSuite
import org.apache.spark.sql.types._

class ParquetRecordUtilSparkTest extends SparkTestSuite {
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

  test("ParquetRecordUtil.GenerateParquetRecords successfully creates Row") { params =>
    val textFilePath = "src/test/resources/singleJsonRowSample.json"
    val inputRDD = params.sc.textFile(textFilePath)

    val records = ParquetRecordUtil.GenerateParquetRecords(inputRDD)
    //println(records._1)

    val rows = records._2.collect
    assert(rows.length == 2)

    val calculatedRowOne = rows(0)
    val calculatedRowTwo = rows(1)
    val expectedArrayOne = Array(123,123,123,123,123,123,123,123,123,123,123,"1.1.1",null,null,null)
    val expectedArrayTwo = Array(null,null,123,123,123,null,null,123,123,123,123,"1.1.1",123,123.456,123)

    expectedArrayOne.zipWithIndex.foreach(element => {
      assert(element._1 == calculatedRowOne.get(element._2))
    })

    expectedArrayTwo.zipWithIndex.foreach(element => {
      assert(element._1 == calculatedRowTwo.get(element._2))
    })

    val structFields = Array(
      StructField("exploded_overlay",LongType,true),
      StructField("exploded_action",LongType,true),
      StructField("device_id",LongType,false),
      StructField("exploded_device_id",LongType,false),
      StructField("exploded_client_event_timestamp",LongType,false),
      StructField("exploded_element",LongType,true),
      StructField("exploded_screen",LongType,true),
      StructField("user_id",LongType,false),
      StructField("server_event_timestamp",LongType,false),
      StructField("app_id",LongType,false),
      StructField("exploded_user_id",LongType,false),
      StructField("client_app_version",StringType,false),
      StructField("exploded_beacon_id",LongType,true),
      StructField("exploded_signal_strength",DoubleType,true),
      StructField("exploded_event_type",LongType,true)
    )
    val expectedStructType = StructType(structFields)

    assert(expectedStructType.equals(records._1))
  }
}
