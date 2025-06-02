package com.example.demo

import org.apache.spark.sql.SparkSession

fun pipeline(
    spark: SparkSession,
    args: Array<String>,
) {
    val file = args.getParameter("--file=")
    val table = args.getParameter("--table=")

    val fileDataset = readFile(spark, file)
    writeTable(fileDataset, table)
}

fun Array<String>.getParameter(key: String): String =
    checkNotNull(this.getOptionParameter(key)) { "Parameter '$key' not found in arguments." }

fun Array<String>.getOptionParameter(key: String): String? = this.find { it.startsWith(key) }?.substringAfter(key)
