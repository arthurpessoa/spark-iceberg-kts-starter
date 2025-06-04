package com.example.demo

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

fun pipeline(
    spark: SparkSession,
    args: Array<String>,
) {
    val file = args.getParameter("--file=")
    val table = args.getParameter("--table=")
    val bootstrapServers = args.getParameter("--bootstrap-servers=")


    readKafkaStream(spark, bootstrapServers, "file_control")
        ?.writeStream()
        ?.format("console")
        ?.outputMode("append")
        ?.start()
        ?.awaitTermination()

    val fileDataset = readFile(spark, file)
    writeTable(fileDataset, table)
}

fun Array<String>.getParameter(key: String): String =
    checkNotNull(this.getOptionParameter(key)) { "Parameter '$key' not found in arguments." }

fun Array<String>.getOptionParameter(key: String): String? = this.find { it.startsWith(key) }?.substringAfter(key)

fun readKafkaStream(spark: SparkSession, bootstrapServers: String, topic: String): Dataset<Row?>? {
    return spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
}
