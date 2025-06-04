package com.example.demo

import org.apache.avro.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

fun readKafkaStreamAvro(
    spark: SparkSession,
    options: Options,
): Dataset<Row> =
    spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", options.bootstrapServers)
        .option("subscribe", options.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .select(from_avro(col("value"), spark.readFileAsString(options.inputSchema)).alias("data"))
        .select("data.*")

fun writeToIceberg(
    dataset: Dataset<Row>,
    options: Options,
) {
    dataset.write()
        .format("iceberg")
        .mode("append")
        .saveAsTable(options.table)
}

fun writeToConsole(dataset: Dataset<Row>) {
    dataset.write()
        .format("console")
        .mode("append")
        .save()
}

fun SparkSession.readFileAsString(filePath: String): String =
    FileSystem.get(sparkContext().hadoopConfiguration())
        .open(Path(filePath))
        .bufferedReader()
        .use { it.readText() }

fun renameColumnsFromAliases(
    df: Dataset<Row>,
    avroSchemaJson: String,
): Dataset<Row> {
    val schema = Schema.Parser().parse(avroSchemaJson)
    var renamedDf = df
    for (field in schema.fields) {
        for (alias in field.aliases()) {
            if (df.columns().contains(alias)) {
                renamedDf = renamedDf.withColumnRenamed(alias, field.name())
            }
        }
    }
    return renamedDf
}
