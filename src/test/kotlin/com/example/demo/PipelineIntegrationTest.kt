package com.example.demo

import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties
import kotlin.test.assertTrue

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
    fun `pipeline runs end-to-end`() = runBlocking {
        // Prepare
        val testFile = "src/test/resources/sample.csv"
        val tableName = "test_iceberg_table_pipeline"
        val topic = "test-topic"
        val avroSchemaPath = "src/main/resources/user.avsc"
        val avroSchema = Schema.Parser().parse(Files.readString(Paths.get(avroSchemaPath)))

        // Create topic using Kafka AdminClient
        val props = Properties()
        props["bootstrap.servers"] = "localhost:${EmbeddedKafkaConfig.defaultKafkaPort()}"
        AdminClient.create(props).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 1, 1))).all().get()
        }

        // Produce Avro message to Kafka
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:${EmbeddedKafkaConfig.defaultKafkaPort()}"
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.name
        KafkaProducer<String, ByteArray>(producerProps).use { producer ->
            val user: GenericRecord = GenericData.Record(avroSchema).apply {
                put("id", 1)
                put("name", "Alice")
                put("age", 30)
            }
            val out = ByteArrayOutputStream()
            val encoder: Encoder = EncoderFactory.get().binaryEncoder(out, null)
            val writer: DatumWriter<GenericRecord> = SpecificDatumWriter(avroSchema)
            writer.write(user, encoder)
            encoder.flush()
            val avroBytes = out.toByteArray()
            producer.send(ProducerRecord(topic, null, avroBytes)).get()
        }

        spark.sql("CREATE TABLE IF NOT EXISTS $tableName (id INT, name STRING, age INT) USING iceberg")

        // Act: run pipeline in a coroutine
        val job = launch(Dispatchers.Default) {
            pipeline(
                spark,
                arrayOf(
                    "--file=$testFile",
                    "--table=$tableName",
                    "--bootstrap-servers=localhost:${EmbeddedKafkaConfig.defaultKafkaPort()}",
                    "--topic=$topic",
                    "--avro-schema=$avroSchemaPath"
                )
            )
        }

        // Await until the table has data or timeout
        await.atMost(10, java.util.concurrent.TimeUnit.SECONDS).until {
            spark.table(tableName).count() > 0
        }

        // Assert
        val resultDf = spark.table(tableName)
        assertTrue(resultDf.count() > 0, "Table should have data")

        // Clean up
        spark.streams().active().forEach { it.stop() }
        job.cancelAndJoin()
    }
}
