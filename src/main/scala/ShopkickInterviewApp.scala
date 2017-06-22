import argparser.ShopkickInterviewAppArgParser
import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}
import parser.ParquetRecordUtil

object ShopkickInterviewApp extends App with Logging {
  implicit val sparkConf = new SparkConf()
    .setIfMissing("spark.master", "local[4]")
    .setIfMissing("spark.executor.memory", "2g")
    .set("spark.logConf", "true")

  implicit val sc = SparkContext.getOrCreate(sparkConf)

  val cliArgs = ShopkickInterviewAppArgParser.parseArgs(args)
  logger.info(s"Running with CLI args: $cliArgs")

  val outputUri = cliArgs.outputUri
  val inputRDD = sc.textFile(cliArgs.inputUri)
  val (schema, records) = ParquetRecordUtil.GenerateParquetRecords(inputRDD)

  val ss = SparkSession.builder().getOrCreate()
  val df = ss.createDataFrame(records, schema)
  df.printSchema()
  df.foreach(println(_))

  df.write.save(cliArgs.outputUri)
}