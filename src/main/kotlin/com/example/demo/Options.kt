package com.example.demo

data class Options(
    val topic: String,
    val table: String,
    val bootstrapServers: String,
    val inputSchema: String,
    val outputSchema: String,
    val checkpointLocation: String,
)

fun buildOptionsFromArgs(args: Array<String>): Options {
    val topic = args.getParameter("--topic=")
    val table = args.getParameter("--table=")
    val bootstrapServers = args.getParameter("--bootstrap-servers=")
    val inputSchema = args.getParameter("--input-schema=")
    val outputSchema = args.getParameter("--output-schema=")
    val checkpointLocation = args.getParameter("--checkpoint-location=")

    return Options(
        topic = topic,
        table = table,
        bootstrapServers = bootstrapServers,
        inputSchema = inputSchema,
        outputSchema = outputSchema,
        checkpointLocation = checkpointLocation,
    )
}

fun Array<String>.getParameter(key: String): String =
    checkNotNull(this.getOptionParameter(key)) { "Parameter '$key' not found in arguments." }

fun Array<String>.getOptionParameter(key: String): String? = this.find { it.startsWith(key) }?.substringAfter(key)
