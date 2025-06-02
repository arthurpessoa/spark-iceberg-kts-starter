package com.example.demo

import org.apache.spark.sql.SparkSession

/**
 * Main entry point for the File2Iceberg application.
 *
 * This application reads a CSV file and writes its contents to an Iceberg table.
 *
 * Usage:
 *   --file=<path_to_csv_file> --table=<iceberg_table_name>
 */
fun main(args: Array<String>) {
    val spark = SparkSession.builder().appName("File2Iceberg").getOrCreate()

    pipeline(spark, args)

    spark.stop()
}
