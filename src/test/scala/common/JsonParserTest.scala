package common


import java.io.{StringWriter, File}

import _root_.grizzled.slf4j.Logging
import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecordBuilder, GenericData, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroSchemaConverter, AvroParquetWriter, AvroRecordConverter}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkContext, SparkConf}

import org.scalatest.FunSuite
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.codehaus.jackson.map.ObjectMapper
import parser.{ParquetRecordUtil, ShopKickSpecificJsonParserUtil}
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import schema.SchemaBuilder
import grizzled.slf4j.Logging

class JsonParserTest extends FunSuite with Logging {

  val sampleJson =
    """
      {
        "app_id": 123,
        "user_id": 123,
        "client_app_version": "1.1.1",
        "device_id": 123,
        "server_event_timestamp": 123,
        "events": [
          {
            "user_id": 123,
            "device_id": 123,
            "action": 123,
            "screen": 123,
            "overlay": 123,
            "element": 123,
            "client_event_timestamp": 123
          },
          {
            "user_id": 123,
            "device_id": 123,
            "event_type": 123,
            "beacon_id": 123,
            "signal_strength": 123.456,
            "client_event_timestamp": 123
          }
        ]
      }
    """
//
//  test("ff") {
//    //    val result = parse(sampleJson)
//    //
//    //    //Assumes that there is no nested data in the root keys aside from that of "events"
//    //    println("THESE ARE THE FIELDS THAT I CAN SEE: ")
//    //    println("----------------------------------------------------------")
//    //    val jsonMap = result.values.asInstanceOf[Map[String, Object]]
//    //
//    //    //TODO: Convert this map to a tuple of parameters... then add that tuple to all of the fields in the avro schemas.
//    //    val elems = jsonMap.filter(x => x._1 != "events")
//    //
//    //    println(elems)
//    //
//    //    println("----------------------------------------------------------")
//    //
//    //    val events = result \ "events"
//    //
//    //    val mapOfEventData = events.children
//    //      .map(x => {
//    //        val explodedMap = x.values.asInstanceOf[Map[String, Object]].map(x => ("exploded_" + x._1, x._2))
//    //        val jsonString = new ObjectMapper().writeValueAsString((explodedMap ++ elems).asJava)
//    //        parse(jsonString)
//    //      })
//    //
//    //    mapOfEventData.foreach(println)
//    //
//
//    val sparkConf = new SparkConf().setAppName("SparkSessionZipsExample").setMaster("local[4]")
//    implicit val sc = SparkContext.getOrCreate(sparkConf)
//    val inputRDD = sc.textFile("src/test/resources/singleJsonRowSample.json")
//
//    val myFlattenedRDD = inputRDD.flatMap(ShopKickSpecificJsonParserUtil.parseAndFlattenJsonEvents(_))
//
//    //val mapOfEventData = ShopKickSpecificJsonParserUtil.parseAndFlattenJsonEvents(sampleJson)
//    //val myRDD = sc.parallelize(mapOfEventData)
//
//    val foo = new SchemaBuilder("my.name.space")
//    val serializedSchema = foo.inferFrom(myFlattenedRDD)
//
//
//    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
//    println(serializedSchema)
//    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
//
//
//    val (schema, records) = ParquetRecordUtil.GenerateParquetRecords(inputRDD)
//
//    val ss = SparkSession.builder().getOrCreate()
//    val df = ss.createDataFrame(records, schema)
//    df.printSchema()
//    df.foreach(println(_))
//
//  }
}
