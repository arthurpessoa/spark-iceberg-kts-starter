package com.example.demo

import org.apache.spark.sql.SparkSession

fun main(args: Array<String>) {
    val options = buildOptionsFromArgs(args)
    val spark = SparkSession.builder().appName("File2Iceberg").getOrCreate()
    pipeline(spark, options)
    spark.stop()
}
