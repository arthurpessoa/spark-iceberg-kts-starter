package com.example.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col
import java.nio.file.Files
import java.nio.file.Paths



fun pipeline(
    spark: SparkSession,
    args: Array<String>,
) {
    val file = args.getParameter("--file=")
    val table = args.getParameter("--table=")
    val topic = args.getParameter("--topic=")
    val bootstrapServers = args.getParameter("--bootstrap-servers=")
    val avroSchemaPath = args.getParameter("--avro-schema=")

    readKafkaStreamAvro(spark, bootstrapServers, topic, avroSchemaPath)
        .writeStream()
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()

    // val fileDataset = readFile(spark, file)
    // writeTable(fileDataset, table)
}

fun Array<String>.getParameter(key: String): String =
    checkNotNull(this.getOptionParameter(key)) { "Parameter '$key' not found in arguments." }

fun Array<String>.getOptionParameter(key: String): String? = this.find { it.startsWith(key) }?.substringAfter(key)
