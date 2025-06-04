package com.example.demo

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

fun pipeline(
    spark: SparkSession,
    options: Options,
) {
    readKafkaStreamAvro(spark = spark, options)
        .writeStream()
        // If any write fails, throw an exception. Spark will not commit the offsets for that batch, and will retry.
        .foreachBatch { batch: Dataset<Row>, _: Long ->
            val renamedDataset = renameColumnsFromAliases(batch, spark.readFileAsString(options.outputSchema))

            writeToConsole(renamedDataset)
            writeToIceberg(renamedDataset, options)
        }
        .start()
        .awaitTermination()
}
