package com.example.demo

import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.spark.sql.SparkSession
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.Properties
import java.util.concurrent.TimeUnit

class PipelineIntegrationTest {
    companion object {
        private lateinit var spark: SparkSession

        @JvmStatic
        @BeforeAll
        fun setup() {
            EmbeddedKafka.start(EmbeddedKafkaConfig.defaultConfig())

            spark =
                SparkSession.builder()
                    .appName("TestPipelineIntegration")
                    .master("local[1]")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                    .config("spark.sql.catalog.spark_catalog.warehouse", "build/tmp/iceberg-warehouse")
                    .config("spark.ui.enabled", false)
                    .getOrCreate()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            spark.stop()
            EmbeddedKafka.stop()
        }
    }

    @Test
    fun `pipeline runs end-to-end`() =
        runBlocking {
            // Prepare
            val testFile = "src/test/resources/sample.csv"
            val tableName = "test_iceberg_table_pipeline"
            val topic = "test-topic"

            // Create topic using Kafka AdminClient
            val props = Properties()
            props["bootstrap.servers"] = "localhost:${EmbeddedKafkaConfig.defaultKafkaPort()}"
            AdminClient.create(props).use { admin ->
                admin.createTopics(listOf(NewTopic(topic, 1, 1))).all().get()
            }

            spark.sql("CREATE TABLE IF NOT EXISTS $tableName (id INT, name STRING, age INT) USING iceberg")

            // Act: run pipeline in a coroutine
            val job =
                launch(Dispatchers.Default) {
                    pipeline(
                        spark,
                        arrayOf(
                            "--file=$testFile",
                            "--table=$tableName",
                            "--bootstrap-servers=localhost:${EmbeddedKafkaConfig.defaultKafkaPort()}",
                            "--topic=$topic",
                        ),
                    )
                }

            // Assert
            await.atMost(10, TimeUnit.SECONDS).until {
                spark.table(tableName).count() > 0
            }

            // Clean up
            spark.streams().active().forEach { it.stop() }
            job.cancelAndJoin()
        }
}
