package com.example.demo

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/** * Reads a CSV file into a Spark Dataset.
 *
 * @param spark The SparkSession to use for reading the file.
 * @param file The path to the CSV file.
 * @return A Dataset<Row> containing the data from the CSV file.
 */
fun readFile(
    spark: SparkSession,
    file: String,
): Dataset<Row> = spark.read().option("header", "true").schema(Schemas.tableSchema).csv(file)

/** * Writes a Spark Dataset to an Iceberg table.
 *
 * @param dataset The Dataset<Row> to write to the Iceberg table.
 * @param table The name of the Iceberg table to write to.
 */
fun writeTable(
    dataset: Dataset<Row>,
    table: String,
): Unit = dataset.write().format("iceberg").mode("overwrite").saveAsTable(table)

fun readKafkaStream(
    spark: SparkSession,
    bootstrapServers: String,
    topic: String,
): Dataset<Row> {
    return spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
}
