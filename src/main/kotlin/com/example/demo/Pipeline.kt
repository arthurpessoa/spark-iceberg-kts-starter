package com.example.demo

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/** Pipeline function that reads from Kafka, processes the data, and writes to Iceberg.
 *
 * @param spark The Spark session.
 * @param options The options containing configuration parameters for the pipeline.
 */
fun kafkaToIcebergPipeline(
    spark: SparkSession,
    options: Options,
) {
    val inputSchema = spark.readFileAsString(options.inputSchema)
    val outputSchema = spark.readFileAsString(options.outputSchema)

    readKafkaStreamAvro(
        spark = spark,
        schema = inputSchema,
        bootstrapServers = options.bootstrapServers,
        topic = options.topic,
    ).writeStream()
        // If any write fails, throw an exception. Spark will not commit the offsets for that batch, and will retry.
        .foreachBatch { batch: Dataset<Row>, _: Long ->
            val renamedDataset = renameColumnsFromAliases(batch, outputSchema)

            writeToConsole(renamedDataset)
            writeToIceberg(renamedDataset, options.table)
        }
        .start()
        .awaitTermination()
}
