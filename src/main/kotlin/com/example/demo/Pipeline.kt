package com.example.demo

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

fun pipeline(
    spark: SparkSession,
    options: Options,
) {
    readKafkaStreamAvro(spark = spark, options).writeStream()
        // If any write fails, throw an exception. Spark will not commit the offsets for that batch, and will retry.
        .foreachBatch { batch: Dataset<Row>, _: Long ->

            // TODO: Save into a file control table
            batch.write()
                .format("console")
                .mode("append")
                .save()

            // TODO: Save into an Iceberg table
            batch.write()
                .format("console")
                .mode("append")
                .save()
        }
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()
        .awaitTermination()
}
