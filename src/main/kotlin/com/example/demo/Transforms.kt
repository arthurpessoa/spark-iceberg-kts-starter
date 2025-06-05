package com.example.demo

import org.apache.avro.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

/**
 * Reads a Kafka stream with Avro data and returns a Dataset<Row> with the data parsed according to the provided schema.
 *
 * @param spark The Spark session.
 * @param schema The Avro schema as a string.
 * @param bootstrapServers The Kafka bootstrap servers.
 * @param topic The Kafka topic to subscribe to.
 * @return A Dataset<Row> containing the parsed data from the Kafka stream.
 */
fun readKafkaStreamAvro(
    spark: SparkSession,
    schema: String,
    bootstrapServers: String,
    topic: String,
): Dataset<Row> =
    spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("writeAheadLog.enable", "true")
        .load()
        .select(from_avro(col("value"), schema).alias("data"))
        .select("data.*")

/** * Writes the dataset to an Iceberg table in append mode.
 *
 * @param dataset The dataset to write to the Iceberg table.
 * @param tableName The name of the Iceberg table.
 */
fun writeToIceberg(
    dataset: Dataset<Row>,
    tableName: String,
) {
    dataset.write()
        .format("iceberg")
        .mode("append")
        .saveAsTable(tableName)
}

/**
 * Writes the dataset to the console in append mode.
 *
 * @param dataset The dataset to write to the console.
 */
fun writeToConsole(dataset: Dataset<Row>) {
    dataset.write()
        .format("console")
        .mode("append")
        .save()
}

/**
 * Renames columns in the DataFrame based on the aliases defined in the provided Avro schema.
 *
 * @param df The DataFrame to rename columns in.
 * @param avroSchemaJson The Avro schema as a JSON string.
 * @return A new DataFrame with columns renamed according to the Avro schema aliases.
 */
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

/**
 * Reads a file from the given file path and returns its content as a string.
 *
 * @param filePath The path to the file to read.
 * @return The content of the file as a string.
 */
fun SparkSession.readFileAsString(filePath: String): String =
    FileSystem.get(sparkContext().hadoopConfiguration())
        .open(Path(filePath))
        .bufferedReader()
        .use { it.readText() }
