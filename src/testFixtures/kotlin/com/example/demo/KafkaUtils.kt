package com.example.demo

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
import java.io.ByteArrayOutputStream
import java.util.Properties
import kotlin.collections.set
import kotlin.use

fun kafkaCreateTopic(
    topic: String,
    port: Int,
) {
    val props = Properties()
    props["bootstrap.servers"] = "localhost:$port"
    AdminClient.create(props).use { admin ->
        admin.createTopics(listOf(NewTopic(topic, 1, 1))).all().get()
    }
}

fun kafkaSend(
    avroSchema: Schema?,
    map: MutableMap<String, Any>,
    topic: String,
    port: Int,
) {
    val producerProps = Properties()
    producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:$port"
    producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.name
    KafkaProducer<String, ByteArray>(producerProps).use { producer ->
        val user: GenericRecord =
            GenericData.Record(avroSchema).apply {
                map.forEach {
                    put(it.key, it.value)
                }
            }
        val out = ByteArrayOutputStream()
        val encoder: Encoder = EncoderFactory.get().binaryEncoder(out, null)
        val writer: DatumWriter<GenericRecord> = SpecificDatumWriter(avroSchema)
        writer.write(user, encoder)
        encoder.flush()
        val avroBytes = out.toByteArray()
        producer.send(ProducerRecord(topic, null, avroBytes)).get()
    }
}
