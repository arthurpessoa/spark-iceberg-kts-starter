package com.example.demo

import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PipelineIntegrationTest {

    companion object {
        private lateinit var spark: SparkSession
        val executor = Executors.newSingleThreadExecutor()

        @JvmStatic
        @BeforeAll
        fun setup() {
            spark = SparkSession.builder().appName("TestPipelineIntegration").master("local[1]")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", "build/tmp/iceberg-warehouse")
                .config("spark.ui.enabled", false)
                .getOrCreate()

            EmbeddedKafka.start(EmbeddedKafkaConfig.defaultConfig())
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            spark.streams().active().forEach { it.stop() }
            spark.stop()
            EmbeddedKafka.stop()
        }
    }

    @Test
    fun `pipeline runs end-to-end`() {

        // Prepare
        val testFile = "src/test/resources/sample.csv"
        val tableName = "test_iceberg_table_pipeline"
        val topic = "test-topic"
        val inputSchema = "src/main/resources/schemas/movie_kafka.avsc"
        val outputSchema = "src/main/resources/schemas/movie_table.avsc"
        val avroSchema = Schema.Parser().parse(Files.readString(Paths.get(inputSchema)))
        val bootstrapServers = "localhost:${EmbeddedKafkaConfig.defaultKafkaPort()}"

        val options = Options(
            topic,
            tableName,
            bootstrapServers,
            inputSchema,
            outputSchema,
        )

        // Create topic using Kafka AdminClient
        kafkaCreateTopic(topic, EmbeddedKafkaConfig.defaultKafkaPort())

        val map = mutableMapOf<String, Any>().apply {
            put("id", "1")
            put("title", "The Matrix")
            put("rating", 10)
        }


        spark.sql("DROP TABLE IF EXISTS $tableName")
        spark.sql("CREATE TABLE IF NOT EXISTS $tableName (idt_movie STRING, des_title STRING, num_rating INT) USING iceberg")

        kafkaSend(avroSchema, map, topic, EmbeddedKafkaConfig.defaultKafkaPort())

        executor.submit {
            pipeline(spark, options)
        }

        await.atMost(10, TimeUnit.HOURS).until {
            spark.catalog().refreshTable(tableName)
            spark.table(tableName).show()
            spark.table(tableName).count() > 1L
        }

        executor.shutdown()
    }
}
