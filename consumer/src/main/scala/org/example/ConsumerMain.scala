package org.example

import org.apache.spark.sql.SparkSession


object ConsumerMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
  }
}
