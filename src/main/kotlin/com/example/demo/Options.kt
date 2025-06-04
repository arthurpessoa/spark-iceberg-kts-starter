package com.example.demo

data class Options(
    val topic: String,
    val bootstrapServers: String,
    val avroSchemaPath: String,
)

fun buildFromArgs(args: Array<String>): Options {
    val topic = args.getParameter("--topic=")
    val bootstrapServers = args.getParameter("--bootstrap-servers=")
    val avroSchemaPath = args.getParameter("--avro-schema-path=")

    return Options(
        topic = topic,
        bootstrapServers = bootstrapServers,
        avroSchemaPath = avroSchemaPath,
    )
}

fun Array<String>.getParameter(key: String): String =
    checkNotNull(this.getOptionParameter(key)) { "Parameter '$key' not found in arguments." }

fun Array<String>.getOptionParameter(key: String): String? = this.find { it.startsWith(key) }?.substringAfter(key)
