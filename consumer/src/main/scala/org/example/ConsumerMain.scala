package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object ConsumerMain {

  private val columns = List("CONTACTMSG", "DESTADDR", "LOGTYPE", "RESULT", "SMID", "UD", "time")

  private val schema = StructType(columns.map(col => StructField(col, StringType)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val options = Map[String, String](
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "input_topic"
    )
    val df = spark.readStream.format("kafka").options(options).load().
      selectExpr("cast (value as string) as json")
      .select(from_json(col("json"), schema).as("parsed"))
      .select("parsed.*").toDF(columns: _*)

    val query = df.writeStream.format("console").
      trigger(Trigger.ProcessingTime("1 second")).start()
    query.awaitTermination()
  }
}
