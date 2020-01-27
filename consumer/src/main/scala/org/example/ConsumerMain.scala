package org.example

import java.net.URLDecoder
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object ConsumerMain {

  private val columns = List("CONTACTMSG", "DESTADDR", "LOGTYPE", "RESULT", "SMID", "UD", "time")

  private val schema = StructType(columns.map(col => StructField(col, StringType)))

  def parseTime(df: DataFrame): DataFrame = {
    val parseFunc: String => Timestamp = str => Timestamp.valueOf(LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    df.withColumn("_time", udf(parseFunc).apply(col("time")))
  }

  def parseContactMsg(df: DataFrame): DataFrame = {
    df.
      withColumn("SMS_ARRAY", split(col("CONTACTMSG"), "-")).
      withColumn("SMS_NUMBER", expr("SMS_ARRAY[0]")).
      withColumn("SMS_LEN", expr("SMS_ARRAY[1]")).
      withColumn("SMS_PART", expr("SMS_ARRAY[2]"))
  }

  def decodeUrl(df: DataFrame): DataFrame = {
    val decodeFunc: String => String = url => URLDecoder.decode(url, "UTF8")
    df.withColumn("URL_DECODED", udf(decodeFunc).apply(col("UD")))
  }

  def aggreate(df: DataFrame): DataFrame = {
    df
      .groupBy(
        window(col("_time"), "5 seconds"),
        col("DESTADDR"),
        col("SMS_NUMBER"),
        col("SMS_PART"),
        col("SMS_LEN"),
        col("RESULT")
      ).agg(
      min("_time").as("min_time"),
      max("_time").as("max_time"),
      last("SMS_PART").as("LAST_SMS_PART"),
      last("RESULT").as("LAST_RESULT")
    )
  }

  def markerFull(df: DataFrame): DataFrame = {
    df.withColumn("MARKER_FULL", when(col("SMS_PART") === col("SMS_LEN") &&
      col("RESULT") === lit(0), 1).otherwise(0))
      //      .filter("MARKER_FULL = 1")
      .filter(col("MARKER_FULL") === lit(1))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    val options = Map[String, String](
      "kafka.bootstrap.servers" -> "localhost:9093",
      "subscribe" -> "input_topic"
    )
    val df = spark.readStream.format("kafka").options(options).load().
      selectExpr("cast (value as string) as json")
      .select(from_json(col("json"), schema).as("parsed"))
      .select("parsed.*").toDF(columns: _*)

    val query = df.
      transform(parseTime).
      transform(parseContactMsg).
      transform(decodeUrl).
      transform(aggreate).
//      transform(markerFull).
      writeStream.format("console").outputMode(OutputMode.Complete()).
      trigger(Trigger.ProcessingTime("5 seconds")).start()

    query.awaitTermination()
  }
}
