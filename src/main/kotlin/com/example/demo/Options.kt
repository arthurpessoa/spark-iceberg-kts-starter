package com.example.demo


data class Options(
    var topic: String = "",
    var table: String = "",
    var bootstrapServers: String = "",
    val inputSchema: String = "",
    val outputSchema: String = "",
)

fun buildOptionsFromArgs(args: Array<String>): Options {
    val topic = args.getParameter("--topic=")
    val table = args.getParameter("--table=")
    val bootstrapServers = args.getParameter("--bootstrap-servers=")
    val inputSchema = args.getParameter("--input-schema=")
    val outputSchema = args.getParameter("--output-schema=")

    return Options(
        topic = topic,
        table = table,
        bootstrapServers = bootstrapServers,
        inputSchema = inputSchema,
        outputSchema = outputSchema,
    )
}

fun Array<String>.getParameter(key: String): String =
    checkNotNull(this.getOptionParameter(key)) { "Parameter '$key' not found in arguments." }

fun Array<String>.getOptionParameter(key: String): String? = this.find { it.startsWith(key) }?.substringAfter(key)
