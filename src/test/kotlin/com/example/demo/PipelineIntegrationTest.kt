package com.example.demo

import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.awaitility.kotlin.await
import org.junit.jupiter.api.*
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.SECONDS

class PipelineIntegrationTest {
    companion object {
        private lateinit var spark: SparkSession

        @JvmStatic
        @BeforeAll
        fun setup() {
            spark =
                SparkSession.builder()
                    .appName("TestPipelineIntegration")
                    .master("local[1]")
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
        val avroSchemaPath = "src/main/resources/user.avsc"
        val avroSchema = Schema.Parser().parse(Files.readString(Paths.get(avroSchemaPath)))
        val bootstrapServers = "localhost:${EmbeddedKafkaConfig.defaultKafkaPort()}"

        val options = Options(
            topic,
            bootstrapServers,
            avroSchemaPath,
        )

        // Create topic using Kafka AdminClient
        kafkaCreateTopic(topic, EmbeddedKafkaConfig.defaultKafkaPort())

        val map =
            mutableMapOf<String, Any>().apply {
                put("id", 1)
                put("name", "Alice")
                put("age", 30)
            }

        // Produce Avro message to Kafka
        kafkaSend(avroSchema, map, topic, EmbeddedKafkaConfig.defaultKafkaPort())

        spark.sql("CREATE TABLE IF NOT EXISTS $tableName (id INT, name STRING, age INT) USING iceberg")
        spark.sql("DELETE FROM $tableName")


        await.atMost(10, SECONDS).until {
            spark.table(tableName).count() > 0L
        }

        runBlocking {
            // Act: run pipeline in a coroutine
            launch(Dispatchers.Default) {
                pipeline(spark, options)
            }
        }
    }
}
